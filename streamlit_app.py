import streamlit as st
import pandas as pd
from pathlib import Path

# ------------------------------------------------------------------
# 0  Page config
# ------------------------------------------------------------------
st.set_page_config(page_title="🏀 CBB Daily Predictions", layout="wide")

"""
### College Basketball Predictions — Daily View

*Run locally ➜ `streamlit run streamlit_app.py`*

This dashboard displays your model’s game‑by‑game predictions, organised one
page per date.

Required columns in the CSV:

* **date** – `YYYY-MM-DD` (or a `game_date` column — auto‑detected)
* **home_team_name**, **away_team_name**
* **pred_margin** – model spread (positive = home favoured)
* **pred_home_win_prob** – home win probability (0‑1)

By default the app looks for **`preds_2025.csv`** in the working directory, but
you can point it at any other file via the sidebar.
"""

# ------------------------------------------------------------------
# 1  Data loader (cached)
# ------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def load_predictions(path: Path) -> pd.DataFrame:
    """Read the CSV and standardise column names/dtypes."""

    if not path.exists():
        st.error(f"🚫 CSV not found: {path}")
        st.stop()

    df = pd.read_csv(path)

    # Identify the date column automatically ("date" or "game_date")
    date_col = None
    for cand in ("date", "game_date", "gamedate", "game_dt"):
        if cand in df.columns:
            date_col = cand
            break
    if date_col is None:
        st.error("CSV must contain a date column (e.g. 'date' or 'game_date').")
        st.stop()

    # Standardise date column name & dtype
    df.rename(columns={date_col: "date"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    expected = {
        "away_team_name",
        "home_team_name",
        "pred_margin",
        "pred_home_win_prob",
        "date",
    }
    missing = expected - set(df.columns)
    if missing:
        st.error(f"CSV is missing columns: {', '.join(missing)}")
        st.stop()

    return df

# ------------------------------------------------------------------
# 2  Sidebar – global controls
# ------------------------------------------------------------------
st.sidebar.header("📂 Data source")
file_path = st.sidebar.text_input("Predictions CSV", "preds_2025.csv")

with st.sidebar.expander("ℹ️ Table filters", expanded=False):
    min_prob = st.slider("Minimum home‑win probability (%)", 0, 100, 0, step=1)

# ------------------------------------------------------------------
# 3  Load data & apply filters
# ------------------------------------------------------------------
df = load_predictions(Path(file_path))

if min_prob > 0:
    df = df[df["pred_home_win_prob"] * 100 >= min_prob]

unique_dates = sorted(df["date"].unique())

# ------------------------------------------------------------------
# 4  Build a tab for every date
# ------------------------------------------------------------------
tab_objs = st.tabs([d.strftime("%b %d, %Y") for d in unique_dates])

for tab, d in zip(tab_objs, unique_dates):
    with tab:
        daily = df[df["date"] == d].copy().reset_index(drop=True)

        # ---------------- Summary metrics ----------------
        col1, col2 = st.columns(2)
        col1.metric("Games", len(daily))
        col2.metric("Avg win prob (%)", f"{daily['pred_home_win_prob'].mean()*100:.1f}")

        # ---------------- Styled table ----------------
        def _style(_df: pd.DataFrame):
            return (
                _df.style
                .format({
                    "pred_margin": "{:+.1f}",
                    "pred_home_win_prob": "{:.1%}",
                })
            )

        show_cols = [
            "away_team_name",
            "home_team_name",
            "pred_margin",
            "pred_home_win_prob",
        ]

        st.dataframe(_style(daily[show_cols]), use_container_width=True, hide_index=True)

        st.download_button(
            label="💾 Download this date as CSV",
            data=daily.to_csv(index=False).encode("utf-8"),
            file_name=f"predictions_{d}.csv",
            mime="text/csv",
        )
