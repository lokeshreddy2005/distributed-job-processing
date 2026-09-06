from __future__ import annotations

from dataclasses import dataclass

from fastapi import Header, HTTPException, status

from common.config import load_api_keys


@dataclass
class ApiClient:
    key: str
    client_name: str
    rate_limit_per_minute: int
    is_admin: bool


def get_api_client(x_api_key: str | None = Header(default=None)) -> ApiClient:
    if not x_api_key:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Missing X-API-Key header")
    keys = load_api_keys()
    info = keys.get(x_api_key)
    if info is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    return ApiClient(
        key=x_api_key,
        client_name=info["client_name"],
        rate_limit_per_minute=info["rate_limit_per_minute"],
        is_admin=info.get("is_admin", False),
    )
