"""仅用标准库的 HTTP 小助手（urllib），供真实子进程服务测试使用。"""
from __future__ import annotations

import json as _json
import urllib.error
import urllib.request


def request(method: str, url: str, payload: dict | None = None,
            headers: dict | None = None, timeout: float = 5.0):
    data = None
    hdrs = headers or {}
    if payload is not None:
        data = _json.dumps(payload).encode("utf-8")
        hdrs = {"Content-Type": "application/json", **hdrs}
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, (_json.loads(body) if body else None)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        return exc.code, (_json.loads(body) if body else None)
