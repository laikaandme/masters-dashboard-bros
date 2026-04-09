import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from bs4 import BeautifulSoup

# -----------------------------
# Page config
# -----------------------------
st.set_page_config(page_title="Masters Pick'em", page_icon="⛳", layout="wide")

# -----------------------------
# Config
# -----------------------------
APP_DIR = Path(__file__).parent
DATA_DIR = APP_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
DEFAULT_PICKS_FILE = APP_DIR / "Masters Pickem.csv"
DB_PATH = DATA_DIR / "masters_history.sqlite3"
STATE_PATH = DATA_DIR / "latest_scores.json"
ESPN_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/golf/pga/scoreboard"
TOURNAMENT_NAME = "Masters Tournament"
POLL_MINUTES = 10
WINNER_BONUS = -5
REQUEST_TIMEOUT = 20

# -----------------------------
# Data helpers
# -----------------------------
def load_picks_csv(path_or_buffer) -> pd.DataFrame:
    last_error = None

    csv_attempts = [
        {"sep": None, "engine": "python", "encoding": "utf-8-sig"},
        {"sep": ",", "engine": "python", "encoding": "utf-8-sig"},
        {"sep": ";", "engine": "python", "encoding": "utf-8-sig"},
        {"sep": None, "engine": "python", "encoding": "latin-1"},
    ]

    for kwargs in csv_attempts:
        try:
            df = pd.read_csv(path_or_buffer, **kwargs)
            df = df.dropna(how="all")
            df.columns = [str(c).strip() for c in df.columns]
            if not df.empty:
                return df
        except Exception as exc:
            last_error = exc

    try:
        df = pd.read_excel(path_or_buffer)
        df = df.dropna(how="all")
        df.columns = [str(c).strip() for c in df.columns]
        return df
    except Exception as exc:
        if last_error is not None:
            raise ValueError(f"Could not read picks file. CSV error: {last_error}. Excel error: {exc}")
        raise


def normalize_name(value: str) -> str:
    value = str(value).strip()
    value = re.sub(r"\s+", " ", value)
    return value.casefold()


def title_case_name(value: str) -> str:
    value = str(value).strip()
    return re.sub(r"\s+", " ", value)


def find_friend_column(df: pd.DataFrame) -> str:
    candidates = ["friend", "player", "name", "person", "entry"]
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in lower:
            return lower[c]
    return df.columns[0]


def find_tier_columns(df: pd.DataFrame, friend_col: str) -> List[str]:
    tier_like = [c for c in df.columns if c != friend_col and "tier" in c.lower()]
    if len(tier_like) >= 5:
        return sorted(tier_like, key=lambda x: x.lower())[:5]
    remaining = [c for c in df.columns if c != friend_col]
    return remaining[:5]


def coerce_picks_layout(df: pd.DataFrame) -> pd.DataFrame:
    """
    Supports both layouts:
    1) Normal row format:
       Friend | Tier 1 | Tier 2 | Tier 3 | Tier 4 | Tier 5
    2) Transposed format:
       Row 1 = friend names across columns
       Row 2-6 = tier picks across columns
    """
    df = df.copy()
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]

    if df.empty:
        return df

    # Already in expected format
    if len(df.columns) >= 6 and len(df) >= 1:
        friend_col = find_friend_column(df)
        tier_cols = find_tier_columns(df, friend_col)
        if len(tier_cols) >= 5 and len(df) > 1:
            return df

    # Try transposed layout conversion
    # Expected shape like 6 rows x N friend columns, often with a blank top-left cell.
    if len(df) >= 6 and len(df.columns) >= 2:
        first_row = df.iloc[0].tolist()
        friend_names = [str(x).strip() for x in first_row[1:] if str(x).strip() and str(x).strip().lower() != 'nan']
        if friend_names:
            converted_rows = []
            for col_idx, friend_name in enumerate(friend_names, start=1):
                converted_rows.append(
                    {
                        "Friend": friend_name,
                        "Tier 1": df.iloc[1, col_idx] if len(df) > 1 and col_idx < len(df.columns) else None,
                        "Tier 2": df.iloc[2, col_idx] if len(df) > 2 and col_idx < len(df.columns) else None,
                        "Tier 3": df.iloc[3, col_idx] if len(df) > 3 and col_idx < len(df.columns) else None,
                        "Tier 4": df.iloc[4, col_idx] if len(df) > 4 and col_idx < len(df.columns) else None,
                        "Tier 5": df.iloc[5, col_idx] if len(df) > 5 and col_idx < len(df.columns) else None,
                    }
                )
            converted = pd.DataFrame(converted_rows).dropna(how="all")
            if not converted.empty:
                return converted

    return df


def parse_score_text(raw: str) -> Optional[int]:
    if raw is None:
        return None
    text = str(raw).strip().upper()
    text = text.replace("−", "-")
    if text in {"", "--", "-", "WD", "DQ", "CUT"}:
        return None
    if text in {"E", "EVEN"}:
        return 0
    match = re.search(r"[-+]?\d+", text)
    if match:
        return int(match.group(0))
    return None


@dataclass
class GolferScore:
    display_name: str
    score: Optional[int]
    raw_value: str
    fetched_at: str


@dataclass
class EventStatus:
    tournament_name: str
    event_id: Optional[str]
    state: str
    description: str
    detail: str
    completed: bool
    fetched_at: str


# -----------------------------
# Persistence
# -----------------------------
def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS golfer_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetched_at TEXT NOT NULL,
            golfer_key TEXT NOT NULL,
            golfer_name TEXT NOT NULL,
            score INTEGER,
            raw_value TEXT NOT NULL,
            UNIQUE(fetched_at, golfer_key)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS friend_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetched_at TEXT NOT NULL,
            friend_name TEXT NOT NULL,
            total_score INTEGER,
            winner_bonus_applied INTEGER NOT NULL,
            UNIQUE(fetched_at, friend_name)
        )
        """
    )
    conn.commit()
    conn.close()



def save_latest_scores(scores: Dict[str, GolferScore]) -> None:
    payload = {
        k: {
            "display_name": v.display_name,
            "score": v.score,
            "raw_value": v.raw_value,
            "fetched_at": v.fetched_at,
        }
        for k, v in scores.items()
    }
    STATE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")



def load_latest_scores() -> Dict[str, GolferScore]:
    if not STATE_PATH.exists():
        return {}
    try:
        raw = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        return {
            k: GolferScore(
                display_name=v["display_name"],
                score=v.get("score"),
                raw_value=v.get("raw_value", ""),
                fetched_at=v.get("fetched_at", ""),
            )
            for k, v in raw.items()
        }
    except Exception:
        return {}



def latest_snapshot_time() -> Optional[str]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT MAX(fetched_at) FROM golfer_snapshots")
    value = cur.fetchone()[0]
    conn.close()
    return value



def persist_golfer_snapshot(scores: Dict[str, GolferScore]) -> None:
    if not scores:
        return
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for golfer_key, item in scores.items():
        cur.execute(
            """
            INSERT OR IGNORE INTO golfer_snapshots (fetched_at, golfer_key, golfer_name, score, raw_value)
            VALUES (?, ?, ?, ?, ?)
            """,
            (item.fetched_at, golfer_key, item.display_name, item.score, item.raw_value),
        )
    conn.commit()
    conn.close()



def persist_friend_snapshot(friend_scores: pd.DataFrame, fetched_at: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for _, row in friend_scores.iterrows():
        cur.execute(
            """
            INSERT OR IGNORE INTO friend_snapshots (fetched_at, friend_name, total_score, winner_bonus_applied)
            VALUES (?, ?, ?, ?)
            """,
            (
                fetched_at,
                str(row["Friend"]),
                None if pd.isna(row["Total Score"]) else int(row["Total Score"]),
                int(bool(row["Winner Bonus Applied"])),
            ),
        )
    conn.commit()
    conn.close()



def load_friend_history() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT fetched_at, friend_name, total_score, winner_bonus_applied FROM friend_snapshots ORDER BY fetched_at, friend_name"
    df = pd.read_sql_query(query, conn)
    conn.close()
    if df.empty:
        return df
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], utc=True, errors="coerce")
    return df


# -----------------------------
# Masters scraping
# -----------------------------
def fetch_espn_scoreboard() -> dict:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
    }
    response = requests.get(ESPN_SCOREBOARD_URL, headers=headers, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def extract_scores_from_espn(payload: dict) -> Tuple[Dict[str, GolferScore], EventStatus]:
    fetched_at = datetime.now(timezone.utc).isoformat()
    events = payload.get("events", []) or []

    masters_event = None
    for event in events:
        name = str(event.get("name", "")).strip()
        short_name = str(event.get("shortName", "")).strip()
        if name == TOURNAMENT_NAME or short_name == TOURNAMENT_NAME:
            masters_event = event
            break

    if masters_event is None:
        scoreboard_day = payload.get("day", {}).get("date", "unknown day")
        raise ValueError(f"{TOURNAMENT_NAME} was not present in ESPN scoreboard payload for {scoreboard_day}.")

    competition = (masters_event.get("competitions") or [{}])[0]
    competition_status = competition.get("status", {})
    type_info = competition_status.get("type", {})
    event_status = EventStatus(
        tournament_name=str(masters_event.get("name", TOURNAMENT_NAME)),
        event_id=masters_event.get("id"),
        state=str(type_info.get("state", "")),
        description=str(type_info.get("description", "")),
        detail=str(type_info.get("detail", "")),
        completed=bool(type_info.get("completed", False)),
        fetched_at=fetched_at,
    )

    golfers: Dict[str, GolferScore] = {}
    for competitor in competition.get("competitors", []) or []:
        athlete = competitor.get("athlete", {}) or {}
        display_name = str(
            athlete.get("displayName")
            or athlete.get("fullName")
            or competitor.get("displayName")
            or ""
        ).strip()
        if not display_name:
            continue

        raw_value = str(competitor.get("score", "")).strip()
        golfers[normalize_name(display_name)] = GolferScore(
            display_name=display_name,
            score=parse_score_text(raw_value),
            raw_value=raw_value,
            fetched_at=fetched_at,
        )

    return golfers, event_status


def scores_changed(new_scores: Dict[str, GolferScore], old_scores: Dict[str, GolferScore]) -> bool:
    if not old_scores:
        return True
    if set(new_scores.keys()) != set(old_scores.keys()):
        return True
    for key, new_item in new_scores.items():
        old_item = old_scores.get(key)
        if old_item is None:
            return True
        if new_item.raw_value != old_item.raw_value:
            return True
    return False


# -----------------------------
# Scoring logic
# -----------------------------
def build_friend_scores(
    picks_df: pd.DataFrame,
    live_scores: Dict[str, GolferScore],
    event_completed: bool = False,
) -> pd.DataFrame:
    friend_col = find_friend_column(picks_df)
    tier_cols = find_tier_columns(picks_df, friend_col)

    records = []

    valid_live = {k: v for k, v in live_scores.items() if v.score is not None}
    if event_completed and valid_live:
        best_score = min(v.score for v in valid_live.values())
        tournament_winners = {k for k, v in valid_live.items() if v.score == best_score}
    else:
        tournament_winners = set()

    for _, row in picks_df.iterrows():
        friend_name = str(row[friend_col]).strip()
        total = 0
        missing = False
        winner_bonus_applied = False
        pick_details = []

        for col in tier_cols:
            golfer_name = str(row[col]).strip()
            golfer_key = normalize_name(golfer_name)
            golfer_live = live_scores.get(golfer_key)

            score = golfer_live.score if golfer_live else None
            raw_value = golfer_live.raw_value if golfer_live else ""
            display_name = golfer_live.display_name if golfer_live else golfer_name

            if score is None:
                missing = True
            else:
                total += score
                if golfer_key in tournament_winners:
                    winner_bonus_applied = True

            pick_details.append(
                {
                    "tier": col,
                    "golfer": display_name,
                    "score": score,
                    "raw_value": raw_value,
                }
            )

        if winner_bonus_applied and not missing:
            total += WINNER_BONUS

        records.append(
            {
                "Friend": friend_name,
                "Total Score": None if missing else total,
                "Winner Bonus Applied": winner_bonus_applied,
                "Pick Details": pick_details,
            }
        )

    out = pd.DataFrame(records)
    out = out.sort_values(by=["Total Score", "Friend"], na_position="last").reset_index(drop=True)
    out.insert(0, "Rank", out.index + 1)
    return out



def flatten_pick_details(friend_scores: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in friend_scores.iterrows():
        for item in row["Pick Details"]:
            rows.append(
                {
                    "Friend": row["Friend"],
                    "Tier": item["tier"],
                    "Golfer": item["golfer"],
                    "Current Score": item["score"],
                    "Raw Website Value": item["raw_value"],
                }
            )
    return pd.DataFrame(rows)


# -----------------------------
# UI helpers
# -----------------------------
def render_auto_refresh() -> None:
    seconds = POLL_MINUTES * 60
    st.components.v1.html(
        f"""
        <script>
        setTimeout(function() {{
            window.parent.location.reload();
        }}, {seconds * 1000});
        </script>
        """,
        height=0,
    )



def render_history_graph(history_df: pd.DataFrame) -> None:
    st.subheader("Score history")
    if history_df.empty:
        st.info("No history yet. Pull scores once to start building the graph.")
        return

    available = sorted(history_df["friend_name"].dropna().unique().tolist())
    selected = st.multiselect("Show friends", available, default=available)
    filtered = history_df[history_df["friend_name"].isin(selected)].copy()
    if filtered.empty:
        st.warning("Select at least one friend to display the graph.")
        return

    filtered = filtered.dropna(subset=["total_score"])
    fig = px.line(
        filtered,
        x="fetched_at",
        y="total_score",
        color="friend_name",
        markers=True,
        labels={
            "fetched_at": "Time",
            "total_score": "Total score",
            "friend_name": "Friend",
        },
    )
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)



def render_leaderboard(friend_scores: pd.DataFrame) -> None:
    st.subheader("Leaderboard")
    display = friend_scores[["Rank", "Friend", "Total Score", "Winner Bonus Applied"]].copy()
    display["Winner Bonus Applied"] = display["Winner Bonus Applied"].map({True: "Yes", False: "No"})
    st.dataframe(display, use_container_width=True, hide_index=True)



def render_friend_cards(friend_scores: pd.DataFrame) -> None:
    detail_df = flatten_pick_details(friend_scores)
    if detail_df.empty:
        st.info("No picks available to display.")
        return

    display_df = detail_df[["Friend", "Tier", "Golfer", "Current Score", "Raw Website Value"]].copy()

    st.markdown("### Picks")
    for friend in friend_scores["Friend"].dropna().astype(str).tolist():
        one = display_df[display_df["Friend"] == friend].copy()
        if one.empty:
            continue
        st.markdown(f"**{friend}**")
        st.dataframe(one[["Tier", "Golfer", "Current Score", "Raw Website Value"]], use_container_width=True, hide_index=True)

# -----------------------------
# Main app
# -----------------------------
def main() -> None:
    init_db()
    render_auto_refresh()

    st.title("Masters Pick'em Tracker")
    st.caption(
        "No Seba, ever."
    )

    with st.sidebar:
        st.header("Setup")
        st.write(
            "Using local file: Masters Pickem.csv"
        )
        st.write(
            "Expected CSV layout: first column = friend name, next five columns = Tier 1 through Tier 5 picks."
        )
        st.markdown("---")
        # refresh_picks = st.button("Reload picks file")
        fetch_now = st.button("Pull latest Masters scores now", type="primary")
        st.write(f"Auto-refresh is set to every {POLL_MINUTES} minutes.")
        st.markdown("---")
        st.write("Scoring notes")
        st.write("- Lower total is better")
        st.write("- Winner bonus = -5, but only after the tournament is final")
        st.write("- Missing Cut Penalty = +10, but later since i don't know how the hell this unnofficial API handles that")
        st.write("- Live score source = ESPN's unofficial PGA scoreboard JSON")

        #if refresh_picks:
            #st.cache_data.clear()

    picks_source = None
    try:
        if DEFAULT_PICKS_FILE.exists():
            picks_source = str(DEFAULT_PICKS_FILE)
            picks_df = load_picks_csv(DEFAULT_PICKS_FILE)
        else:
            st.info("Place 'Masters Pickem.csv' in the app folder to begin.")
            return
    except Exception as exc:
        st.error(f"Unable to read picks file from {picks_source or 'unknown source'}: {exc}")
        return

    if picks_df.empty:
        st.warning(f"The picks file loaded from {picks_source} has no data rows.")
        if DEFAULT_PICKS_FILE.exists():
            try:
                preview_text = DEFAULT_PICKS_FILE.read_text(encoding='utf-8', errors='replace')[:1200]
                st.code(preview_text or '[file is blank]')
            except Exception:
                pass
        return

    picks_df = coerce_picks_layout(picks_df)
    #st.caption(f"Loaded picks from: {picks_source} · rows: {len(picks_df)} · columns: {len(picks_df.columns)}")
    st.caption(f" {', '.join(picks_df.iloc[:, 0].astype(str).tolist())}")
    with st.expander("Detected picks table"):
        st.dataframe(picks_df, use_container_width=True, hide_index=True)

    live_scores = load_latest_scores()
    status_placeholder = st.empty()
    event_status = None

    if fetch_now:
        try:
            payload = fetch_espn_scoreboard()
            parsed_scores, event_status = extract_scores_from_espn(payload)
            if not parsed_scores:
                status_placeholder.error("ESPN returned the Masters event, but no competitor scores were found.")
            else:
                previous = load_latest_scores()
                if scores_changed(parsed_scores, previous):
                    save_latest_scores(parsed_scores)
                    persist_golfer_snapshot(parsed_scores)
                    live_scores = parsed_scores
                    status_placeholder.success(
                        f"Pulled ESPN scores at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} and stored a new snapshot."
                    )
                else:
                    live_scores = previous
                    status_placeholder.info("Scores were unchanged, so no new history point was added.")
        except Exception as exc:
            status_placeholder.error(f"Failed to pull ESPN Masters scores: {exc}")

    if not live_scores:
        st.warning("No live golfer scores have been stored yet. You can still review the picks below, and once you pull scores the leaderboard and graph will populate.")
        friend_scores = build_friend_scores(picks_df, {}, event_completed=False)
        render_history_graph(load_friend_history())
        st.markdown("---")
        render_friend_cards(friend_scores)
        return

    event_completed = bool(event_status.completed) if event_status else False
    friend_scores = build_friend_scores(picks_df, live_scores, event_completed=event_completed)

    fetched_times = sorted({v.fetched_at for v in live_scores.values() if v.fetched_at})
    fetched_at = fetched_times[-1] if fetched_times else datetime.now(timezone.utc).isoformat()
    persist_friend_snapshot(friend_scores, fetched_at)

    history_df = load_friend_history()
    render_history_graph(history_df)

    if event_status is not None:
        state_text = event_status.description or event_status.state or "Unknown"
        detail_text = event_status.detail or ""
        st.caption(f"Tournament status: {state_text} {('· ' + detail_text) if detail_text else ''}")

    col1, col2 = st.columns([2, 1])
    with col1:
        render_leaderboard(friend_scores)
    with col2:
        valid_scores = friend_scores["Total Score"].dropna()
        if not valid_scores.empty:
            st.metric("Best total", int(valid_scores.min()))
            st.metric("Worst total", int(valid_scores.max()))
        latest_time = latest_snapshot_time()
        st.metric("Latest stored update", latest_time or "None")

    st.markdown("---")
    render_friend_cards(friend_scores)

    with st.expander("Implementation notes"):
        st.write(
            "This app stores snapshots in SQLite plus a latest JSON cache so you can build the score-over-time graph. "
            "It now pulls from ESPN's unofficial PGA scoreboard JSON instead of scraping masters.com directly. "
            "The -5 winner bonus is only applied after ESPN marks the event complete."
        )


if __name__ == "__main__":
    main()
