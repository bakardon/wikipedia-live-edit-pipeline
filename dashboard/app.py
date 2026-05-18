"""Wikipedia Edit Pipeline — Streamlit dashboard (serving layer).

Five tabs cover the full assignment scope:
  1. Live Pulse        — real-time SSE→Kafka→Spark flow (refreshes every N seconds)
  2. Trending & Velocity — window functions (RANK, LAG) from dbt marts
  3. Vandalism Detection — 10-min sliding-window heuristic
  4. Stream vs Batch    — live editing intensity vs. historical popularity (Clickstream)
  5. Pipeline Health    — row counts, freshness, dbt run state
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text
from streamlit_autorefresh import st_autorefresh

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------
PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "wiki")
PG_USER = os.environ.get("POSTGRES_USER", "wiki")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "wiki")

# When tables are missing, show Windows-friendly commands (many laptops have no `make`).
HINT_DBT = (
    "Build models with dbt — **Windows:** `powershell -File scripts/run-dbt.ps1` "
    "(requires `pip install dbt-postgres`; stack must expose Postgres on `localhost:5544`). "
    "**Unix:** `make dbt`."
)

# Curated color palette — gentle on dark bg, distinct categorically
PALETTE = ["#FF6B6B", "#4ECDC4", "#FFE66D", "#95E1D3", "#FFB6B9",
           "#C7CEEA", "#B5EAD7", "#FFDAC1", "#E2D1F9", "#9BB7D4"]
ACCENT = "#FF6B6B"
TEAL = "#4ECDC4"
WARN = "#FFE66D"

# Approximate geo centroid per wiki-language code (for the world map).
WIKI_GEO: dict[str, tuple[float, float]] = {
    "enwiki": (52.5, -1.5),       "enwiktionary": (52.5, -1.5),
    "dewiki": (51.1, 10.4),       "dewiktionary": (51.1, 10.4),
    "frwiki": (46.6, 2.2),        "frwiktionary": (46.6, 2.2),
    "eswiki": (40.4, -3.7),       "eswiktionary": (40.4, -3.7),
    "itwiki": (41.9, 12.6),       "ptwiki": (39.5, -8.0),
    "nlwiki": (52.1, 5.3),        "ruwiki": (61.5, 105.3),
    "zhwiki": (35.0, 105.0),      "zhwiktionary": (35.0, 105.0),
    "jawiki": (36.2, 138.2),      "kowiki": (35.9, 127.8),
    "arwiki": (24.0, 45.0),       "plwiki": (51.9, 19.1),
    "trwiki": (38.9, 35.2),       "viwiki": (14.0, 108.0),
    "idwiki": (-0.8, 113.9),      "thwiki": (15.9, 100.9),
    "hewiki": (31.0, 35.0),       "fawiki": (32.0, 53.0),
    "elwiki": (39.0, 22.0),       "elwiktionary": (39.0, 22.0),
    "ukwiki": (48.4, 31.2),       "cswiki": (49.8, 15.5),
    "huwiki": (47.2, 19.5),       "svwiki": (60.1, 18.6),
    "nowiki": (60.5, 9.0),        "dawiki": (56.0, 10.0),
    "fiwiki": (61.9, 25.7),       "rowiki": (45.9, 25.0),
    "bgwiki": (42.7, 25.5),       "skwiki": (48.7, 19.7),
    "hrwiki": (45.1, 15.2),       "etwiki": (58.6, 25.0),
    "lvwiki": (56.9, 24.6),       "ltwiki": (55.2, 23.9),
    "slwiki": (46.1, 14.5),       "hiwiki": (20.6, 78.9),
    "mswiki": (4.2, 101.9),       "bnwiki": (24.0, 90.0),
    "tawiki": (10.0, 78.0),       "urwiki": (30.4, 69.3),
    "commonswiki": (46.8, 8.2),   "wikidatawiki": (46.8, 8.2),
    "metawiki": (46.8, 8.2),
}

st.set_page_config(
    page_title="Wikipedia Edit Pipeline",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "About": "Hybrid Data Engineering pipeline: live Wikimedia EventStreams "
                 "+ Clickstream batch baseline.",
    },
)

# ----------------------------------------------------------------------------
# CSS polish
# ----------------------------------------------------------------------------
st.markdown(
    """
<style>
[data-testid="stMetric"] {
    background: linear-gradient(135deg, rgba(255,107,107,0.06), rgba(78,205,196,0.06));
    border: 1px solid rgba(255,255,255,0.05);
    padding: 1rem 1.2rem;
    border-radius: 10px;
}
[data-testid="stMetricValue"] { font-size: 1.85rem !important; font-weight: 700 !important; }
[data-testid="stMetricLabel"] p { font-size: 0.85rem !important; color: rgba(255,255,255,0.6) !important; text-transform: uppercase; letter-spacing: 0.06em; }
[data-testid="stMetricDelta"] { font-size: 0.85rem !important; }

.stTabs [data-baseweb="tab-list"] { gap: 6px; border-bottom: 1px solid rgba(255,255,255,0.06); }
.stTabs [data-baseweb="tab"]     { padding: 8px 18px; border-radius: 6px 6px 0 0; }
.stTabs [aria-selected="true"]   { background: rgba(255,107,107,0.10); }

.pill {
    display: inline-block; padding: 3px 10px; border-radius: 12px;
    font-size: 0.75rem; margin: 2px 4px 2px 0;
    background: rgba(78,205,196,0.12); color: #4ECDC4;
    border: 1px solid rgba(78,205,196,0.25);
}
.pill.warn  { background: rgba(255,230,109,0.10); color: #FFE66D; border-color: rgba(255,230,109,0.25); }

hr { margin: 0.8rem 0; }
footer { visibility: hidden; }
#MainMenu { visibility: hidden; }

/* tighter popovers ("⚙️" buttons) */
[data-testid="stPopover"] button { padding: 2px 8px; font-size: 0.9rem; }
</style>
""",
    unsafe_allow_html=True,
)


# ----------------------------------------------------------------------------
# Database helpers
# ----------------------------------------------------------------------------
@st.cache_resource
def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
        pool_pre_ping=True,
        pool_recycle=300,
    )


def q(sql: str, params: dict | None = None, ttl: int = 5) -> pd.DataFrame:
    @st.cache_data(ttl=ttl, show_spinner=False)
    def _inner(sql_inner: str, params_tuple: tuple) -> pd.DataFrame:
        with get_engine().connect() as conn:
            return pd.read_sql(text(sql_inner), conn, params=dict(params_tuple) if params_tuple else {})
    return _inner(sql, tuple(params.items()) if params else ())


def relation_exists(schema: str, name: str) -> bool:
    df = q(
        "SELECT 1 FROM information_schema.tables WHERE table_schema=:s AND table_name=:n",
        {"s": schema, "n": name},
        ttl=30,
    )
    return not df.empty


def safe_table_count(schema: str, name: str) -> int:
    """Row count, or 0 if the relation has not been created yet (e.g. before dbt run)."""
    if not relation_exists(schema, name):
        return 0
    df = q(f"SELECT count(*) AS n FROM {schema}.{name}", ttl=10)
    return int(df["n"].iloc[0]) if not df.empty else 0


def safe_max_ts(schema: str, name: str, column: str = "ingested_at"):
    if not relation_exists(schema, name):
        return None
    df = q(f"SELECT max({column}) AS x FROM {schema}.{name}", ttl=10)
    if df.empty or df["x"].iloc[0] is None or pd.isna(df["x"].iloc[0]):
        return None
    return df["x"].iloc[0]


def fmt_int(n) -> str:
    if n is None or pd.isna(n):
        return "—"
    return f"{int(n):,}"


def fmt_pct(n, digits: int = 1) -> str:
    if n is None or pd.isna(n):
        return "—"
    return f"{n*100:.{digits}f}%"


def chart_header(title: str, description: str, key: str):
    """Render a chart heading with a ⚙️ popover; returns the popover for adding controls."""
    head_col, ctrl_col = st.columns([12, 1])
    with head_col:
        st.markdown(f"### {title}")
    pop = ctrl_col.popover("⚙️", help="Chart settings", use_container_width=True)
    with pop:
        st.caption(description)
    return pop


# ----------------------------------------------------------------------------
# Sidebar
# ----------------------------------------------------------------------------
with st.sidebar:
    st.markdown("# 🌍 Wikipedia Pipeline")
    st.caption("Hybrid Data Engineering • Live EventStreams + Clickstream batch")

    st.markdown("### Refresh")
    refresh_seconds = st.slider(
        "Auto-refresh (sec)", 2, 30, 5,
        help="How often the page re-queries Postgres. Lower = more live, higher = less DB load.",
    )

    st.markdown("### Live filters")
    time_window_min = st.select_slider(
        "Time window", options=[1, 5, 15, 30, 60, 180, 720],
        value=15,
        format_func=lambda x: f"{x}m" if x < 60 else f"{x // 60}h",
        help="Limits Live-tab queries to edits ingested within this trailing window.",
    )

    if relation_exists("raw", "stg_edit"):
        wikis_df = q(
            "SELECT wiki_db, count(*) AS c FROM raw.stg_edit "
            "WHERE ingested_at > now() - interval '24 hours' "
            "GROUP BY wiki_db ORDER BY c DESC LIMIT 60",
            ttl=60,
        )
        wiki_options = ["all"] + wikis_df["wiki_db"].tolist()
        selected_wiki = st.selectbox(
            "Wiki", wiki_options,
            help="`all` = every Wikimedia project. Pick one to focus all live charts on it.",
        )
    else:
        selected_wiki = "all"

    ns_options = {
        "All namespaces": None,
        "Articles (0)": 0,
        "Talk (1)": 1,
        "User (2)": 2,
        "User talk (3)": 3,
        "Wikipedia/meta (4)": 4,
        "File (6)": 6,
    }
    selected_ns_label = st.selectbox(
        "Namespace", list(ns_options.keys()),
        help="MediaWiki namespace. 0 = article pages; others are user/talk/meta pages.",
    )
    selected_ns = ns_options[selected_ns_label]

    editor_filter = st.radio(
        "Editor type",
        ["any", "humans only (no bots)", "anonymous only", "bots only"],
        index=0,
        help="Bots are flagged in the Wikimedia stream; anonymous editors show as IP addresses.",
    )

    st.markdown("### Stack")
    pills = ["SSE → Kafka", "Spark Streaming", "PostgreSQL 16",
             "dbt 1.9", "GitHub Actions", "Streamlit"]
    st.markdown(" ".join(f'<span class="pill">{p}</span>' for p in pills),
                unsafe_allow_html=True)

    st.markdown("### About")
    st.caption(
        f"Postgres `{PG_HOST}:{PG_PORT}/{PG_DB}`  \n"
        f"Refreshed `{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}`"
    )

st_autorefresh(interval=refresh_seconds * 1000, key="autorefresh")


def live_where() -> tuple[str, dict]:
    """WHERE clause + params for live queries based on sidebar filters."""
    parts = [f"ingested_at > now() - interval '{int(time_window_min)} minutes'"]
    params: dict[str, object] = {}
    if selected_wiki != "all":
        parts.append("wiki_db = :wiki")
        params["wiki"] = selected_wiki
    if selected_ns is not None:
        parts.append("namespace = :ns")
        params["ns"] = selected_ns
    if editor_filter == "humans only (no bots)":
        parts.append("NOT is_bot")
    elif editor_filter == "anonymous only":
        parts.append("is_anon")
    elif editor_filter == "bots only":
        parts.append("is_bot")
    return "WHERE " + " AND ".join(parts), params


# ----------------------------------------------------------------------------
# Header
# ----------------------------------------------------------------------------
st.markdown(
    f"""
<div style="display:flex; align-items:center; justify-content:space-between;">
  <div>
    <h1 style="margin:0; color:{ACCENT};">🌍 Wikipedia Edit Pipeline</h1>
    <p style="margin:0; color:rgba(255,255,255,0.55);">
      Real-time edits from every Wikipedia in every language, with historical Clickstream baseline.
    </p>
  </div>
  <div style="text-align:right;">
    <span class="pill">Stream</span>
    <span class="pill">Batch</span>
    <span class="pill warn">5 layers</span>
  </div>
</div>
""",
    unsafe_allow_html=True,
)
st.divider()


tab_live, tab_window, tab_van, tab_compare, tab_health = st.tabs(
    ["⚡ Live Pulse", "📈 Trending & Velocity", "🛡️ Vandalism",
     "🔀 Stream vs Batch", "🩺 Pipeline Health"]
)

# ============================================================================
# TAB 1: LIVE PULSE
# ============================================================================
with tab_live:
    if not relation_exists("raw", "stg_edit"):
        st.warning("Waiting for the streaming job to land its first batch in `raw.stg_edit`.")
    else:
        where_now, params_now = live_where()

        # ---- KPIs ---------------------------------------------------------
        kpi = q(f"""
            SELECT
                count(*)                                  AS edits,
                count(DISTINCT wiki_db)                   AS wikis,
                count(DISTINCT editor)                    AS editors,
                avg(CASE WHEN is_anon THEN 1.0 ELSE 0 END) AS anon_share,
                avg(CASE WHEN is_bot  THEN 1.0 ELSE 0 END) AS bot_share,
                COALESCE(sum(ABS(bytes_changed)), 0)      AS bytes_churned
            FROM raw.stg_edit {where_now}
        """, params_now, ttl=refresh_seconds)
        r = kpi.iloc[0] if not kpi.empty else None

        kpi_prev = q(f"""
            SELECT count(*) AS edits
            FROM raw.stg_edit
            WHERE ingested_at >  now() - interval '{int(time_window_min)*2} minutes'
              AND ingested_at <= now() - interval '{int(time_window_min)} minutes'
              {("AND wiki_db = :wiki" if selected_wiki != "all" else "")}
              {("AND namespace = :ns" if selected_ns is not None else "")}
        """, params_now, ttl=refresh_seconds)
        prev_edits = int(kpi_prev["edits"].iloc[0]) if not kpi_prev.empty else 0
        delta_edits = (int(r["edits"]) - prev_edits) if r is not None else 0

        col1, col2, col3, col4 = st.columns(4)
        col1.metric(
            f"Edits, last {time_window_min}m",
            fmt_int(r["edits"] if r is not None else 0),
            delta=(f"{delta_edits:+,}" if r is not None else None),
            help="Total edits in the current sidebar window, with delta vs the previous equal-length window.",
        )
        col2.metric(
            "Edits/min",
            fmt_int((r["edits"] / time_window_min) if r is not None else 0),
            help="Average per-minute throughput across the window.",
        )
        col3.metric(
            "Active wikis",
            fmt_int(r["wikis"] if r is not None else 0),
            help="Distinct Wikimedia projects (`wiki_db`) edited in this window.",
        )
        col4.metric(
            "Anon share",
            fmt_pct(r["anon_share"]) if r is not None else "—",
            help="Fraction of edits from anonymous IPs.",
        )

        col5, col6, col7, col8 = st.columns(4)
        col5.metric("Distinct editors", fmt_int(r["editors"] if r is not None else 0),
                    help="Unique user/bot/IP identities in this window.")
        col6.metric("Bot share", fmt_pct(r["bot_share"]) if r is not None else "—",
                    help="Fraction of edits flagged `bot=true` by Wikimedia.")
        col7.metric("Bytes churned",
                    fmt_int(r["bytes_churned"] if r is not None else 0),
                    help="Sum of absolute bytes added/removed across all edits.")
        col8.metric("Window length", f"{time_window_min} min",
                    help="Matches the 'Time window' control in the sidebar.")

        st.divider()

        # ---- Timeline ----------------------------------------------------
        pop = chart_header(
            "Live edit rate",
            "Edits per minute, broken down by editor type. "
            "Uses stacked area by default; switch to lines to compare proportions.",
            "timeline",
        )
        with pop:
            chart_type = st.radio(
                "Chart type", ["Stacked area", "Lines"], index=0, key="tl_type",
                help="Stacked area shows total volume; lines make each series easier to compare.",
            )
            show_bots = st.checkbox("Show bots", value=True, key="tl_bots",
                                    help="Bots dominate volume; hiding them makes human/anon trends easier to see.")

        timeline = q(f"""
            SELECT
                date_trunc('minute', ts) AS minute,
                count(*) FILTER (WHERE NOT is_anon AND NOT is_bot) AS users,
                count(*) FILTER (WHERE is_anon)                    AS anon,
                count(*) FILTER (WHERE is_bot)                     AS bots
            FROM raw.stg_edit {where_now}
            GROUP BY 1 ORDER BY 1
        """, params_now, ttl=refresh_seconds)

        if timeline.empty:
            st.info("No edits in this window yet — try widening it in the sidebar.")
        else:
            fig = go.Figure()
            mode = "lines"
            fill_kw = dict(stackgroup="one") if chart_type == "Stacked area" else dict()
            fig.add_trace(go.Scatter(x=timeline["minute"], y=timeline["users"], name="Users",
                                     mode=mode, line=dict(color=TEAL, width=2),
                                     fillcolor="rgba(78,205,196,0.20)", **fill_kw))
            fig.add_trace(go.Scatter(x=timeline["minute"], y=timeline["anon"], name="Anonymous",
                                     mode=mode, line=dict(color=ACCENT, width=2),
                                     fillcolor="rgba(255,107,107,0.20)", **fill_kw))
            if show_bots:
                fig.add_trace(go.Scatter(x=timeline["minute"], y=timeline["bots"], name="Bots",
                                         mode=mode, line=dict(color=WARN, width=2),
                                         fillcolor="rgba(255,230,109,0.20)", **fill_kw))
            fig.update_layout(
                template="plotly_dark", height=320,
                margin=dict(t=10, b=10, l=10, r=10),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                xaxis_title=None, yaxis_title="edits/min",
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                hovermode="x unified",
            )
            st.plotly_chart(fig, use_container_width=True)

        # ---- Map + Top wikis ---------------------------------------------
        col_left, col_right = st.columns([3, 2])

        with col_left:
            pop = chart_header(
                "Edit activity by language (geo)",
                "Each circle is one Wikimedia project plotted at its primary-language centroid; "
                "size + color encode edit volume in the window.",
                "map",
            )
            with pop:
                proj = st.selectbox("Projection",
                                    ["natural earth", "equirectangular", "orthographic", "mollweide"],
                                    index=0, key="map_proj",
                                    help="`natural earth` is the balanced default; `orthographic` is a 3-D globe.")
                cs = st.selectbox("Color scale", ["Plasma", "Viridis", "Inferno", "Turbo"],
                                  index=0, key="map_cs", help="Continuous palette for circle color.")

            geo = q(f"""
                SELECT wiki_db, count(*) AS edits
                FROM raw.stg_edit {where_now}
                GROUP BY wiki_db
            """, params_now, ttl=refresh_seconds)
            if geo.empty:
                st.info("No data yet.")
            else:
                geo["lat"] = geo["wiki_db"].map(lambda w: WIKI_GEO.get(w, (None, None))[0])
                geo["lon"] = geo["wiki_db"].map(lambda w: WIKI_GEO.get(w, (None, None))[1])
                mappable = geo.dropna(subset=["lat", "lon"]).copy()
                if mappable.empty:
                    st.info("No mapped wikis in this window — try a longer time window.")
                else:
                    fig = px.scatter_geo(
                        mappable, lat="lat", lon="lon",
                        size="edits", hover_name="wiki_db",
                        hover_data={"edits": True, "lat": False, "lon": False},
                        size_max=40, color="edits", color_continuous_scale=cs,
                        projection=proj,
                    )
                    fig.update_geos(
                        showcountries=True, countrycolor="rgba(255,255,255,0.15)",
                        showcoastlines=True, coastlinecolor="rgba(255,255,255,0.2)",
                        showland=True, landcolor="rgba(255,255,255,0.04)",
                        showocean=True, oceancolor="rgba(0,0,0,0)",
                        bgcolor="rgba(0,0,0,0)",
                    )
                    fig.update_layout(
                        template="plotly_dark", height=380,
                        margin=dict(t=10, b=10, l=0, r=0),
                        paper_bgcolor="rgba(0,0,0,0)",
                        coloraxis_showscale=False,
                    )
                    st.plotly_chart(fig, use_container_width=True)

        with col_right:
            pop = chart_header(
                "Top wikis",
                "Horizontal bar of the most-edited Wikimedia projects in the window.",
                "topwikis",
            )
            with pop:
                top_n_wikis = st.slider("Top N", 5, 30, 15, key="topwikis_n",
                                        help="Number of wikis to show, ranked by edit count.")
                bar_color = st.selectbox("Color scale", ["Plasma", "Viridis", "Turbo"],
                                         index=0, key="topwikis_cs")

            top_wikis = q(f"""
                SELECT wiki_db, count(*) AS edits
                FROM raw.stg_edit {where_now}
                GROUP BY wiki_db ORDER BY edits DESC LIMIT {int(top_n_wikis)}
            """, params_now, ttl=refresh_seconds)
            if top_wikis.empty:
                st.info("No data.")
            else:
                fig = px.bar(top_wikis, x="edits", y="wiki_db", orientation="h",
                             color="edits", color_continuous_scale=bar_color)
                fig.update_layout(
                    template="plotly_dark", height=380,
                    margin=dict(t=10, b=10, l=0, r=0),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    yaxis={"categoryorder": "total ascending", "title": None},
                    xaxis_title="edits",
                    coloraxis_showscale=False, showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True)

        # ---- Namespace + Editor mix --------------------------------------
        col_a, col_b = st.columns(2)
        with col_a:
            pop = chart_header(
                "Namespace mix",
                "Edit distribution across MediaWiki namespaces (0 = Article, 1 = Talk, …).",
                "ns",
            )
            with pop:
                ns_kind = st.radio("View", ["Donut", "Bar"], index=0, key="ns_kind",
                                   help="Donut emphasises proportions; bar emphasises absolute counts.")

            ns = q(f"""
                SELECT
                  CASE namespace
                    WHEN 0 THEN '0 — Articles'   WHEN 1 THEN '1 — Talk'
                    WHEN 2 THEN '2 — User'        WHEN 3 THEN '3 — User talk'
                    WHEN 4 THEN '4 — Wikipedia'   WHEN 6 THEN '6 — File'
                    WHEN 10 THEN '10 — Template'  WHEN 14 THEN '14 — Category'
                    ELSE namespace::text || ' — other'
                  END AS ns_name,
                  count(*) AS edits
                FROM raw.stg_edit {where_now}
                GROUP BY 1 ORDER BY edits DESC
            """, params_now, ttl=refresh_seconds)
            if ns.empty:
                st.info("No data.")
            else:
                if ns_kind == "Donut":
                    fig = px.pie(ns.head(8), values="edits", names="ns_name", hole=0.55,
                                 color_discrete_sequence=PALETTE)
                    fig.update_traces(textposition="inside", textinfo="percent")
                else:
                    fig = px.bar(ns.head(8), x="ns_name", y="edits",
                                 color="ns_name", color_discrete_sequence=PALETTE)
                    fig.update_layout(xaxis_title=None, yaxis_title="edits", showlegend=False)
                fig.update_layout(
                    template="plotly_dark", height=300,
                    margin=dict(t=10, b=10, l=10, r=10),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig, use_container_width=True)

        with col_b:
            pop = chart_header(
                "Editor mix",
                "Three classes: registered users, bots (flag set by Wikimedia), and anonymous IPs.",
                "edmix",
            )
            with pop:
                ed_kind = st.radio("View", ["Donut", "Bar"], index=0, key="ed_kind",
                                   help="Donut for proportions, bar for counts.")

            ed = q(f"""
                SELECT
                  CASE WHEN is_bot THEN 'Bot' WHEN is_anon THEN 'Anonymous' ELSE 'Registered user' END AS kind,
                  count(*) AS edits
                FROM raw.stg_edit {where_now}
                GROUP BY 1 ORDER BY edits DESC
            """, params_now, ttl=refresh_seconds)
            if ed.empty:
                st.info("No data.")
            else:
                colors = {"Registered user": TEAL, "Bot": WARN, "Anonymous": ACCENT}
                if ed_kind == "Donut":
                    fig = px.pie(ed, values="edits", names="kind", hole=0.55,
                                 color="kind", color_discrete_map=colors)
                    fig.update_traces(textposition="inside", textinfo="percent+label")
                else:
                    fig = px.bar(ed, x="kind", y="edits",
                                 color="kind", color_discrete_map=colors)
                    fig.update_layout(xaxis_title=None, yaxis_title="edits", showlegend=False)
                fig.update_layout(
                    template="plotly_dark", height=300,
                    margin=dict(t=10, b=10, l=10, r=10),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig, use_container_width=True)

        # ---- Recent edits ticker -----------------------------------------
        pop = chart_header(
            "Recent edits (live ticker)",
            "Latest edits as they arrive — one row per Wikipedia revision.",
            "ticker",
        )
        with pop:
            ticker_n = st.slider("Rows", 10, 100, 25, key="ticker_n",
                                 help="Number of most-recent rows to show.")

        ticker = q(f"""
            SELECT
                ts, wiki_db, page_title, editor,
                CASE WHEN is_bot THEN '🤖 bot'
                     WHEN is_anon THEN '👤 anon'
                     ELSE '👨‍💻 user' END AS who,
                bytes_changed, comment
            FROM raw.stg_edit {where_now}
            ORDER BY ts DESC LIMIT {int(ticker_n)}
        """, params_now, ttl=refresh_seconds)
        if ticker.empty:
            st.info("No recent edits.")
        else:
            st.dataframe(
                ticker, use_container_width=True, hide_index=True,
                column_config={
                    "ts": st.column_config.DatetimeColumn("UTC time", format="HH:mm:ss"),
                    "wiki_db": "Wiki", "page_title": "Page", "editor": "Editor",
                    "who": st.column_config.TextColumn("Type", width="small"),
                    "bytes_changed": st.column_config.NumberColumn("Δ bytes", format="%d"),
                    "comment": st.column_config.TextColumn("Comment", width="large"),
                },
            )

# ============================================================================
# TAB 2: TRENDING & VELOCITY
# ============================================================================
with tab_window:
    if not relation_exists("marts", "trending_per_minute"):
        st.warning(f"`marts.trending_per_minute` not built yet — {HINT_DBT}")
    else:
        pop = chart_header(
            "Trending pages — RANK window",
            "Output of `RANK() OVER (PARTITION BY minute_bucket ORDER BY edits DESC)`. "
            "Pages ranked within each 1-minute tumbling window across all wikis.",
            "trending",
        )
        with pop:
            trend_window_min = st.slider("Look back (min)", 5, 360, 60, key="trend_window",
                                         help="Aggregates ranks over this many minutes back.")
            trend_topn = st.slider("Top N pages", 5, 40, 15, key="trend_topn",
                                   help="Number of pages to display in the bar chart.")
            trend_topk = st.slider("Per-minute rank cutoff", 1, 10, 3, key="trend_topk",
                                   help="In the table below, show only pages with rank ≤ this within a minute.")

        trending = q(f"""
            SELECT
                t.minute_bucket, t.wiki_db, t.page_title, t.edit_count,
                t.rank_in_minute, t.rank_in_wiki_minute
            FROM marts.trending_per_minute t
            WHERE t.minute_bucket >= now() - interval '{int(trend_window_min)} minutes'
              AND t.rank_in_minute <= {int(max(trend_topk, 10))}
            ORDER BY t.minute_bucket DESC, t.rank_in_minute ASC
            LIMIT 500
        """, ttl=15)

        if trending.empty:
            st.info("No trending data in this window.")
        else:
            top_pages = (trending.groupby(["wiki_db", "page_title"], as_index=False)["edit_count"]
                                  .sum()
                                  .sort_values("edit_count", ascending=False).head(int(trend_topn)))
            fig = px.bar(top_pages, x="edit_count", y="page_title", orientation="h",
                         color="wiki_db", color_discrete_sequence=PALETTE,
                         hover_data=["wiki_db"])
            fig.update_layout(
                template="plotly_dark", height=440,
                margin=dict(t=10, b=10, l=10, r=10),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                yaxis={"categoryorder": "total ascending", "title": None},
                xaxis_title="cumulative edits in window",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title=None),
            )
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("###### Per-minute rankings (filterable)")
            st.dataframe(
                trending[trending["rank_in_minute"] <= int(trend_topk)].head(60),
                use_container_width=True, hide_index=True,
                column_config={
                    "minute_bucket": st.column_config.DatetimeColumn("Minute", format="HH:mm"),
                    "wiki_db": "Wiki", "page_title": "Page",
                    "edit_count": st.column_config.ProgressColumn("Edits", min_value=0, max_value=10, format="%d"),
                    "rank_in_minute":      st.column_config.NumberColumn("Global rank", format="#%d"),
                    "rank_in_wiki_minute": st.column_config.NumberColumn("Wiki rank",   format="#%d"),
                },
            )

    st.divider()

    if not relation_exists("marts", "edit_velocity"):
        st.warning(f"`marts.edit_velocity` not built — {HINT_DBT}")
    else:
        pop = chart_header(
            "Edit velocity — LAG window",
            "Output of `LAG(edit_count, 60) OVER (PARTITION BY page_id ORDER BY minute_bucket)`. "
            "Pages whose edit rate is **rising** vs. 1 hour ago — a leading indicator of breaking news.",
            "velocity",
        )
        with pop:
            vel_hours = st.slider("Look back (hours)", 1, 24, 6, key="vel_hours",
                                  help="Time range over which to evaluate acceleration.")
            vel_min_accel = st.number_input("Min Δ", min_value=1, max_value=100, value=1, key="vel_min",
                                            help="Only show pages whose this-minute count exceeds 1h-ago count by this much.")
            vel_n = st.slider("Show top", 5, 100, 30, key="vel_n",
                              help="Number of accelerating pages to show.")

        accel = q(f"""
            SELECT v.minute_bucket, v.wiki_db, v.page_title,
                   v.edits_this_min, v.edits_lag_60, v.accel
            FROM marts.edit_velocity v
            WHERE v.minute_bucket >= now() - interval '{int(vel_hours)} hours'
              AND v.accel IS NOT NULL
              AND v.accel >= {int(vel_min_accel)}
            ORDER BY v.accel DESC, v.minute_bucket DESC
            LIMIT {int(vel_n)}
        """, ttl=15)
        if accel.empty:
            st.info("No accelerating pages in this window yet.")
        else:
            st.dataframe(
                accel, use_container_width=True, hide_index=True,
                column_config={
                    "minute_bucket": st.column_config.DatetimeColumn("Minute", format="HH:mm"),
                    "wiki_db": "Wiki", "page_title": "Page",
                    "edits_this_min": st.column_config.NumberColumn("Now/min", format="%d"),
                    "edits_lag_60":   st.column_config.NumberColumn("60m ago", format="%d"),
                    "accel":          st.column_config.NumberColumn("Δ (now − lag)", format="%+d"),
                },
            )

# ============================================================================
# TAB 3: VANDALISM
# ============================================================================
with tab_van:
    if not relation_exists("marts", "vandalism_candidates"):
        st.warning(f"`marts.vandalism_candidates` not built — {HINT_DBT}")
    else:
        pop = chart_header(
            "10-min sliding-window vandalism heuristic",
            "Pages with ≥3 anonymous edits OR ≥30% revert share within a 10-minute window. "
            "Same signature Wikipedia patrollers use.",
            "vand",
        )
        with pop:
            van_hours = st.slider("Look back (hours)", 1, 48, 24, key="van_hours",
                                  help="Time range to scan.")
            sev_filter = st.multiselect(
                "Severity",
                ["🔴 high", "🟡 medium", "🟢 low"],
                default=["🔴 high", "🟡 medium", "🟢 low"],
                key="van_sev",
                help="High = ≥6 anon edits or ≥50% revert; medium = ≥4 / ≥40%; low = the rest.",
            )
            van_n = st.slider("Max rows", 10, 100, 30, key="van_n")

        van = q(f"""
            SELECT v.window_start, v.window_end, v.wiki_db, v.page_title,
                   v.anon_edit_count, v.total_edit_count, v.distinct_ip_classes, v.revert_share
            FROM marts.vandalism_candidates v
            WHERE v.window_end >= now() - interval '{int(van_hours)} hours'
            ORDER BY v.anon_edit_count DESC, v.revert_share DESC NULLS LAST
            LIMIT {int(van_n)}
        """, ttl=15)

        if van.empty:
            st.info(
                "No vandalism candidates in the selected window. Heuristic needs ≥3 anon edits "
                "in a 10-min window or revert share ≥30%."
            )
        else:
            col_a, col_b, col_c = st.columns(3)
            col_a.metric("Candidate windows", fmt_int(len(van)))
            col_b.metric("Avg anon edits/window", f"{van['anon_edit_count'].mean():.1f}")
            col_c.metric("Avg revert share", fmt_pct(van["revert_share"].mean()))

            van["severity"] = van.apply(
                lambda r: "🔴 high" if r["anon_edit_count"] >= 6 or (r["revert_share"] or 0) >= 0.5
                else ("🟡 medium" if r["anon_edit_count"] >= 4 or (r["revert_share"] or 0) >= 0.4 else "🟢 low"),
                axis=1,
            )
            van = van[van["severity"].isin(sev_filter)] if sev_filter else van

            st.dataframe(
                van[["severity", "window_start", "wiki_db", "page_title",
                     "anon_edit_count", "total_edit_count", "distinct_ip_classes", "revert_share"]],
                use_container_width=True, hide_index=True,
                column_config={
                    "severity": st.column_config.TextColumn("Severity", width="small"),
                    "window_start": st.column_config.DatetimeColumn("Window start", format="MMM D HH:mm"),
                    "wiki_db": "Wiki", "page_title": "Page",
                    "anon_edit_count": st.column_config.ProgressColumn("Anon edits", min_value=0, max_value=10, format="%d"),
                    "total_edit_count": st.column_config.NumberColumn("Total", format="%d"),
                    "distinct_ip_classes": st.column_config.NumberColumn("/24 classes", format="%d"),
                    "revert_share": st.column_config.NumberColumn("Revert share", format="%.0f%%"),
                },
            )

# ============================================================================
# TAB 4: STREAM vs BATCH
# ============================================================================
with tab_compare:
    if not relation_exists("core", "fact_clickstream"):
        st.warning("`core.fact_clickstream` is not built yet (needs Clickstream batch + dbt).")
        st.markdown("**Windows (PowerShell), from repo root:**")
        st.code("powershell -File scripts/setup-clickstream.ps1", language="powershell")
        st.markdown("**macOS / Linux (Make):**")
        st.code(
            "make download-clickstream\nmake batch\nmake dbt",
            language="bash",
        )
    else:
        n_cs = q("SELECT count(*) AS n FROM core.fact_clickstream", ttl=120)
        n_rows = int(n_cs["n"].iloc[0]) if not n_cs.empty else 0
        if n_rows == 0:
            st.warning("Clickstream table exists but has 0 rows — re-run batch, then dbt.")
            st.code(
                "powershell -File scripts/run-batch.ps1\npowershell -File scripts/run-dbt.ps1",
                language="powershell",
            )
            st.caption("Unix: `make batch && make dbt`")
        else:
            colA, colB = st.columns([2, 1])
            with colA:
                pop = chart_header(
                    "Top pages by historical navigations (batch)",
                    "From the Wikipedia Clickstream monthly dump — most-visited target pages.",
                    "hist",
                )
                with pop:
                    hist_n = st.slider("Top N", 10, 100, 25, key="hist_n")
                    hist_cs = st.selectbox("Color scale", ["Viridis", "Plasma", "Turbo"],
                                           index=0, key="hist_cs")

                top_hist = q(f"""
                    SELECT p.page_title, sum(fc.nav_count)::bigint AS navigations
                    FROM core.fact_clickstream fc
                    JOIN core.dim_page p ON p.page_id = fc.target_page_id
                    GROUP BY p.page_title
                    ORDER BY navigations DESC LIMIT {int(hist_n)}
                """, ttl=300)
                fig = px.bar(top_hist, x="navigations", y="page_title", orientation="h",
                             color="navigations", color_continuous_scale=hist_cs)
                fig.update_layout(
                    template="plotly_dark", height=520,
                    margin=dict(t=10, b=10, l=10, r=10),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    yaxis={"categoryorder": "total ascending", "title": None},
                    coloraxis_showscale=False,
                )
                st.plotly_chart(fig, use_container_width=True)

            with colB:
                st.markdown("##### Coverage")
                st.metric("Clickstream rows", fmt_int(n_rows))
                month = q("SELECT min(dump_month) AS m, max(dump_month) AS mx FROM core.fact_clickstream", ttl=300)
                if not month.empty:
                    st.metric("Months covered",
                              f"{month['m'].iloc[0]} → {month['mx'].iloc[0]}")
                pop = chart_header(
                    "Nav type",
                    "Where the visitor came from: another Wikipedia page (`link`), an external site (`external`), or search/other.",
                    "navtype",
                )
                nav_types = q("""
                    SELECT nav_type, sum(nav_count)::bigint AS n
                    FROM core.fact_clickstream GROUP BY 1 ORDER BY 2 DESC
                """, ttl=300)
                if not nav_types.empty:
                    fig = px.pie(nav_types, values="n", names="nav_type", hole=0.55,
                                 color_discrete_sequence=PALETTE)
                    fig.update_layout(
                        template="plotly_dark", height=260,
                        margin=dict(t=10, b=10, l=10, r=10),
                        paper_bgcolor="rgba(0,0,0,0)", showlegend=True,
                    )
                    st.plotly_chart(fig, use_container_width=True)

            st.divider()
            pop = chart_header(
                "Anomaly score — live edits ÷ historical popularity",
                "Pages getting edited disproportionately to their normal traffic — candidate breaking-news events.",
                "anom",
            )
            with pop:
                anom_hours = st.slider("Live window (hours)", 1, 24, 1, key="anom_hours",
                                       help="Count live edits over this trailing range.")
                anom_min_live = st.slider("Min live edits", 1, 50, 2, key="anom_min",
                                          help="Filter out pages with fewer live edits than this.")
                anom_n = st.slider("Show top", 5, 50, 20, key="anom_n")

            anomalies = q(f"""
                WITH live AS (
                    SELECT p.page_title, count(*) AS live_edits
                    FROM raw.stg_edit s
                    JOIN core.dim_page p
                      ON p.wiki_id   = s.wiki_db
                     AND p.namespace = s.namespace
                     AND p.page_title = s.page_title
                    WHERE s.ts >= now() - interval '{int(anom_hours)} hours'
                      AND s.wiki_db = 'enwiki'
                      AND s.namespace = 0
                    GROUP BY p.page_title
                    HAVING count(*) >= {int(anom_min_live)}
                ),
                hist AS (
                    SELECT p.page_title, sum(fc.nav_count)::bigint AS navigations
                    FROM core.fact_clickstream fc
                    JOIN core.dim_page p ON p.page_id = fc.target_page_id
                    GROUP BY p.page_title
                )
                SELECT l.page_title, l.live_edits,
                       COALESCE(h.navigations, 0) AS navigations,
                       l.live_edits::numeric / GREATEST(COALESCE(h.navigations, 1), 1) AS anomaly_score
                FROM live l
                LEFT JOIN hist h USING (page_title)
                ORDER BY anomaly_score DESC
                LIMIT {int(anom_n)}
            """, ttl=60)
            if anomalies.empty:
                st.info("Not enough matching live enwiki edits yet — try a longer live window.")
            else:
                st.dataframe(
                    anomalies, use_container_width=True, hide_index=True,
                    column_config={
                        "page_title": "Page",
                        "live_edits": st.column_config.NumberColumn("Live edits", format="%d"),
                        "navigations": st.column_config.NumberColumn("Historical nav", format="%d"),
                        "anomaly_score": st.column_config.NumberColumn(
                            "Anomaly", format="%.4f",
                            help="live ÷ historical — higher is more anomalous",
                        ),
                    },
                )


# ============================================================================
# TAB 5: PIPELINE HEALTH
# ============================================================================
with tab_health:
    pop = chart_header(
        "Pipeline health & freshness",
        "Row counts and most-recent ingest time across every layer.",
        "health",
    )

    try:
        r = {
            "stg_edit_rows": safe_table_count("raw", "stg_edit"),
            "stg_edit_latest": safe_max_ts("raw", "stg_edit"),
            "stg_cs_rows": safe_table_count("raw", "stg_clickstream"),
            "dim_wiki_rows": safe_table_count("core", "dim_wiki"),
            "dim_page_rows": safe_table_count("core", "dim_page"),
            "dim_editor_rows": safe_table_count("core", "dim_editor"),
            "fact_edit_rows": safe_table_count("core", "fact_edit"),
            "fact_cs_rows": safe_table_count("core", "fact_clickstream"),
            "trending_rows": safe_table_count("marts", "trending_per_minute"),
            "velocity_rows": safe_table_count("marts", "edit_velocity"),
            "vandalism_rows": safe_table_count("marts", "vandalism_candidates"),
            "dim_time_rows": safe_table_count("core", "dim_time"),
        }
    except Exception as exc:
        r = None
        st.error(f"Could not read pipeline health from Postgres ({exc}).")

    if r is None:
        pass
    else:
        st.markdown("##### Raw landing (Spark streaming sink)")
        col1, col2, col3 = st.columns(3)
        col1.metric("raw.stg_edit rows", fmt_int(r["stg_edit_rows"]),
                    help="Append-only landing for live SSE → Spark → Postgres.")
        col2.metric("raw.stg_clickstream rows", fmt_int(r["stg_cs_rows"]),
                    help="Append-only landing for the monthly Clickstream batch.")
        latest = r["stg_edit_latest"]
        col3.metric("Last ingest",
                    (latest.strftime("%H:%M:%S UTC") if latest else "—"),
                    help="Time of the most-recent row from Spark Structured Streaming.")

        st.markdown("##### Core warehouse (dbt-built Kimball star)")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("dim_wiki",   fmt_int(r["dim_wiki_rows"]),
                  help="Distinct Wikimedia projects.")
        c2.metric("dim_page",   fmt_int(r["dim_page_rows"]),
                  help="Distinct (wiki, namespace, title) pages seen.")
        c3.metric("dim_editor", fmt_int(r["dim_editor_rows"]),
                  help="Distinct editors: users, bots, anonymous IPs.")
        c4.metric("dim_time",   fmt_int(r["dim_time_rows"]),
                  help="Pre-seeded minute-grain for the project year.")

        d1, d2 = st.columns(2)
        d1.metric("fact_edit",        fmt_int(r["fact_edit_rows"]),
                  help="Star-schema fact: one row per Wikipedia revision.")
        d2.metric("fact_clickstream", fmt_int(r["fact_cs_rows"]),
                  help="Star-schema fact: monthly navigation counts.")

        st.markdown("##### Marts (window-function outputs)")
        m1, m2, m3 = st.columns(3)
        m1.metric("trending_per_minute", fmt_int(r["trending_rows"]),
                  help="RANK() OVER (PARTITION BY minute_bucket ORDER BY edits DESC)")
        m2.metric("edit_velocity",       fmt_int(r["velocity_rows"]),
                  help="LAG(edit_count, 60) OVER (PARTITION BY page_id ORDER BY minute_bucket)")
        m3.metric("vandalism_candidates", fmt_int(r["vandalism_rows"]),
                  help="Sliding 10-min window grouped by (page, /24 IP class)")

    st.markdown("##### Architecture")
    st.code(
        """
Wikimedia EventStreams (SSE, 30-100 events/sec)
        │
        ▼
[ingestion/sse_to_kafka.py]  ── User-Agent + pydantic schema validation
        │
        ▼
Kafka topic  wiki.edits  (3 partitions, KRaft)
        │
        ▼
[streaming/stream_job.py]  Spark Structured Streaming (--master local[2])
        │
        ▼
PostgreSQL 16  ──>  raw.stg_edit  (append-only landing)
                              │   dbt run
                              ▼
                    core.dim_wiki, core.dim_page, core.dim_editor,
                    core.fact_edit (Kimball star, deterministic md5 surrogates)
                              │   dbt run
                              ▼
                    marts.trending_per_minute   (RANK window)
                    marts.edit_velocity         (LAG  window)
                    marts.vandalism_candidates  (10-min sliding window)
                              │
                              ▼
                    Streamlit dashboard (this page)

Batch source:
  Wikipedia Clickstream (478 MB/month)
        ▼
[batch/clickstream_load.py]  Spark batch  ──>  raw.stg_clickstream
                                              │   dbt run
                                              ▼
                                          core.fact_clickstream
""",
        language="text",
    )

    st.markdown("##### Connection")
    st.caption(f"Postgres at `{PG_HOST}:{PG_PORT}/{PG_DB}` as `{PG_USER}`")


# ----------------------------------------------------------------------------
# Footer
# ----------------------------------------------------------------------------
st.markdown(
    """
<div style="text-align:center; padding-top:1.5rem; color:rgba(255,255,255,0.4); font-size:0.85rem;">
University Data Engineering final project · Kafka 3.7 · Spark 3.5 · Postgres 16 · dbt 1.9 · Streamlit 1.36
</div>
""",
    unsafe_allow_html=True,
)
