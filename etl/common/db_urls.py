from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


def _infer_neon_endpoint(hostname: str) -> str | None:
    host = str(hostname or "").strip().lower()
    if not host:
        return None
    if host.startswith("ep-"):
        return host
    return None


def rewrite_tunneled_database_url(raw: str, *, host: str = "127.0.0.1", port: int = 6543) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    parsed = urlparse(value)
    if not parsed.scheme or not parsed.netloc:
        return value

    query_items = parse_qsl(parsed.query, keep_blank_values=True)
    seen_channel_binding = False
    endpoint_present = False
    rewritten_query: list[tuple[str, str]] = []

    for key, query_value in query_items:
        key_text = str(key or "")
        value_text = str(query_value or "")
        if key_text == "channel_binding":
            rewritten_query.append((key_text, "disable"))
            seen_channel_binding = True
            continue
        if key_text == "options" and "endpoint=" in value_text:
            endpoint_present = True
        rewritten_query.append((key_text, value_text))

    if not seen_channel_binding:
        rewritten_query.append(("channel_binding", "disable"))

    endpoint = _infer_neon_endpoint(parsed.hostname or "")
    if endpoint and not endpoint_present:
        appended = False
        for index, (key_text, value_text) in enumerate(rewritten_query):
            if key_text != "options":
                continue
            joiner = "" if not value_text.strip() else " "
            rewritten_query[index] = (key_text, f"{value_text}{joiner}endpoint={endpoint}".strip())
            appended = True
            break
        if not appended:
            rewritten_query.append(("options", f"endpoint={endpoint}"))

    userinfo = ""
    if parsed.username is not None:
        userinfo = parsed.username
        if parsed.password is not None:
            userinfo += ":" + parsed.password
        userinfo += "@"
    netloc = f"{userinfo}{str(host or '127.0.0.1').strip() or '127.0.0.1'}:{int(port or 6543)}"
    query = urlencode(rewritten_query, doseq=True)
    return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, query, parsed.fragment))


__all__ = ["rewrite_tunneled_database_url"]
