CREATE TABLE IF NOT EXISTS wiki_changes (
    id          UUID PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL,
    wiki        TEXT NOT NULL,
    title       TEXT NOT NULL,
    user_name   TEXT,
    is_bot      BOOLEAN NOT NULL,
    change_type TEXT NOT NULL,   -- edit / new
    size_delta  INTEGER          -- octets ajoutés ou retirés
);

CREATE INDEX IF NOT EXISTS wiki_changes_ts_idx ON wiki_changes (ts);
