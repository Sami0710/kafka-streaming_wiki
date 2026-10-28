KEPT_TYPES = {"edit", "new"}


def parse_sse_line(line: str) -> tuple[str, str] | None:
    """Découpe une ligne SSE "champ: valeur"."""
    if not line or line.startswith(":"):
        return None
    field, _, value = line.partition(":")
    if value.startswith(" "):
        value = value[1:]
    return field, value


def to_event(raw: dict) -> dict | None:
    """Garde les champs utiles d'une modification, ou None si on l'ignore."""
    meta = raw.get("meta") or {}
    if meta.get("domain") == "canary":  # événements de monitoring de Wikimedia
        return None
    if raw.get("type") not in KEPT_TYPES:
        return None
    if not (meta.get("id") and meta.get("dt") and raw.get("wiki") and raw.get("title")):
        return None

    length = raw.get("length") or {}
    size_delta = (length.get("new") or 0) - (length.get("old") or 0) if length else None
    return {
        "id": meta["id"],
        "ts": meta["dt"],
        "wiki": raw["wiki"],
        "title": raw["title"],
        "user": raw.get("user"),
        "bot": bool(raw.get("bot")),
        "type": raw["type"],
        "size_delta": size_delta,
    }
