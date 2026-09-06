"""Real job handlers — genuinely time-consuming, deterministic, CPU/IO-bound
work, so idempotency (same input -> byte-identical output) and throughput
numbers are both meaningful. No sleep() stubs.

Each handler returns (result_dict, effect_key, result_hash):
  - effect_key: where the durable side effect lives (a file path, mostly)
  - result_hash: a hash of the *content* of that side effect, used by the
    idempotency ledger (common.models.SideEffect) and by tests/experiments to
    prove a retried job produced byte-identical output, not a corrupted one.
"""
from __future__ import annotations

import hashlib
import io
import random
import time
from pathlib import Path

from PIL import Image, ImageFilter

from common.config import settings


class JobFailure(Exception):
    """Raised by a handler to signal a legitimate, retryable job failure."""


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _should_force_fail(payload: dict, attempt: int) -> bool:
    """force_fail=true fails every attempt (until retries are exhausted).
    force_fail_until_attempt=N fails only attempts <= N, then lets it
    succeed — used to demonstrate a job that recovers on retry. The check
    happens after the real work below, not before, so there is always a
    genuine multi-second processing window to kill a worker inside of."""
    if payload.get("force_fail_until_attempt") is not None:
        return attempt <= int(payload["force_fail_until_attempt"])
    return bool(payload.get("force_fail"))


def run_prime_calc(job_id: str, payload: dict, attempt: int = 1) -> tuple[dict, str, str]:
    """CPU-bound: counts primes below `limit` via trial division (deliberately
    unoptimized so duration is controllable and easy to reason about for load
    testing — a sieve would finish before we could observe anything)."""
    limit = int(payload.get("limit", 50_000))

    t0 = time.perf_counter()
    count = 0
    primes: list[int] = []
    for n in range(2, limit):
        is_prime = True
        for p in primes:
            if p * p > n:
                break
            if n % p == 0:
                is_prime = False
                break
        if is_prime:
            primes.append(n)
            count += 1
    elapsed_ms = (time.perf_counter() - t0) * 1000

    if _should_force_fail(payload, attempt):
        raise JobFailure(f"prime_calc: forced failure on attempt {attempt} (after {elapsed_ms:.0f}ms of real work)")

    result = {"limit": limit, "prime_count": count, "elapsed_ms": round(elapsed_ms, 2)}
    payload_bytes = f"{limit}:{count}".encode()
    result_hash = _sha256(payload_bytes)

    out_dir = Path(settings.results_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{job_id}.prime.json"
    out_path.write_text(f'{{"limit": {limit}, "prime_count": {count}}}')

    return result, str(out_path), result_hash


def run_image_resize(job_id: str, payload: dict, attempt: int = 1) -> tuple[dict, str, str]:
    """CPU+IO-bound: procedurally generates a deterministic source image from
    the job_id (so no upload plumbing is needed for load testing), applies a
    Gaussian blur, and produces multiple real thumbnail sizes with Pillow."""
    base_size = int(payload.get("base_size", 1200))
    sizes = payload.get("sizes", [512, 256, 128, 64])

    seed = int(hashlib.sha256(job_id.encode()).hexdigest(), 16) % (2**32)
    rng = random.Random(seed)

    img = Image.new("RGB", (base_size, base_size))
    pixels = img.load()
    # deterministic procedural noise pattern — genuinely touches every pixel
    for y in range(0, base_size, 4):
        for x in range(0, base_size, 4):
            color = (rng.randint(0, 255), rng.randint(0, 255), rng.randint(0, 255))
            for dy in range(4):
                for dx in range(4):
                    if x + dx < base_size and y + dy < base_size:
                        pixels[x + dx, y + dy] = color

    img = img.filter(ImageFilter.GaussianBlur(radius=3))

    out_dir = Path(settings.results_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    written_hashes = []
    for size in sizes:
        thumb = img.resize((size, size), Image.LANCZOS)
        buf = io.BytesIO()
        thumb.save(buf, format="PNG")
        data = buf.getvalue()
        written_hashes.append(_sha256(data))
        out_path = out_dir / f"{job_id}.{size}.png"
        out_path.write_bytes(data)

    combined_hash = _sha256("".join(written_hashes).encode())

    if _should_force_fail(payload, attempt):
        raise JobFailure(f"image_resize: forced failure on attempt {attempt} (after real resize work)")

    result = {
        "base_size": base_size,
        "sizes": sizes,
        "output_files": [f"{job_id}.{s}.png" for s in sizes],
        "combined_hash": combined_hash,
    }
    primary_path = out_dir / f"{job_id}.{sizes[0]}.png"
    return result, str(primary_path), combined_hash


HANDLERS = {
    "prime_calc": run_prime_calc,
    "image_resize": run_image_resize,
}
