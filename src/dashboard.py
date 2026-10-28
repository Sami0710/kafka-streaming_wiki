import pandas as pd
import psycopg
import streamlit as st

from config import DATABASE_URL

st.set_page_config(page_title="Wikipédia en direct", layout="wide")


@st.cache_resource
def get_conn():
    return psycopg.connect(DATABASE_URL, autocommit=True)


def query(sql: str) -> pd.DataFrame:
    with get_conn().cursor() as cur:
        cur.execute(sql)
        columns = [col.name for col in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)


st.title("Wikipédia en direct")
st.caption("Modifications Wikipédia en temps réel (Kafka -> PostgreSQL), rafraîchi toutes les 5 s.")


@st.fragment(run_every=5)
def live() -> None:
    kpi = query("""
        SELECT
            count(*) FILTER (WHERE ts > now() - interval '1 minute') AS last_min,
            avg(is_bot::int) FILTER (WHERE ts > now() - interval '10 minutes') AS bot_share,
            count(DISTINCT wiki) FILTER (WHERE ts > now() - interval '10 minutes') AS wikis,
            count(*) AS total
        FROM wiki_changes
    """).iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Modifications, dernière minute", f"{int(kpi.last_min):,}".replace(",", " "))
    c2.metric("Part de robots (10 min)", "-" if pd.isna(kpi.bot_share) else f"{float(kpi.bot_share):.0%}")
    c3.metric("Wikis actifs (10 min)", int(kpi.wikis))
    c4.metric("Total enregistré", f"{int(kpi.total):,}".replace(",", " "))

    st.subheader("Modifications par minute")
    per_minute = query("""
        SELECT date_trunc('minute', ts) AS minute,
               count(*) FILTER (WHERE NOT is_bot) AS "Humains",
               count(*) FILTER (WHERE is_bot) AS "Robots"
        FROM wiki_changes
        WHERE ts > now() - interval '30 minutes' AND ts < date_trunc('minute', now())
        GROUP BY 1 ORDER BY 1
    """)
    if per_minute.empty:
        st.info("Pas encore assez de données (il faut au moins une minute complète).")
    else:
        st.line_chart(per_minute.set_index("minute"))

    left, right = st.columns(2)
    with left:
        st.subheader("Wikis les plus actifs (10 min)")
        wikis = query("""
            SELECT wiki, count(*) AS modifications
            FROM wiki_changes WHERE ts > now() - interval '10 minutes'
            GROUP BY wiki ORDER BY modifications DESC LIMIT 10
        """)
        st.dataframe(
            wikis,
            hide_index=True,
            use_container_width=True,
            column_config={
                "modifications": st.column_config.ProgressColumn(
                    "modifications", format="%d", min_value=0,
                    max_value=int(wikis.modifications.max()) if not wikis.empty else 1,
                ),
            },
        )
    with right:
        st.subheader("Pages les plus modifiées par des humains (1 h)")
        pages = query("""
            SELECT wiki, title AS page, count(*) AS modifications
            FROM wiki_changes WHERE ts > now() - interval '1 hour' AND NOT is_bot
            GROUP BY wiki, title ORDER BY modifications DESC LIMIT 10
        """)
        st.dataframe(pages, hide_index=True, use_container_width=True)

    st.subheader("Dernières modifications")
    latest = query("""
        SELECT to_char(ts, 'HH24:MI:SS') AS heure, wiki, title AS page, user_name AS auteur,
               CASE WHEN is_bot THEN 'robot' ELSE 'humain' END AS type, size_delta AS octets
        FROM wiki_changes ORDER BY ts DESC LIMIT 15
    """)
    st.dataframe(latest, hide_index=True, use_container_width=True)


live()
