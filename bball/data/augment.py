"""Home / away augmentation to remove positional home-team bias during training.

Adapted from hoops-edge-predictor.  For every original row a mirror row is
appended where home and away features are swapped, the spread is negated, and
the win label is flipped.  This forces the model to learn team-strength
differences rather than which slot a team occupies.
"""
from __future__ import annotations

import pandas as pd

# Columns whose home/away counterpart has a *different* naming convention.
_SPECIAL_PAIRS = {
    "home_opp_ft_rate": "away_def_ft_rate",
    "away_def_ft_rate": "home_opp_ft_rate",
}


def _build_swap_pairs(columns: list[str]) -> list[tuple[str, str]]:
    """Return (col_a, col_b) pairs whose values should be exchanged."""
    pairs: list[tuple[str, str]] = []
    seen: set[str] = set()

    for col in columns:
        if col in seen:
            continue

        # 1) Check explicit asymmetric mappings first
        if col in _SPECIAL_PAIRS:
            partner = _SPECIAL_PAIRS[col]
            if partner in columns:
                pairs.append((col, partner))
                seen.update([col, partner])

        # 2) General home_ <-> away_ swap
        elif col.startswith("home_"):
            partner = "away_" + col[len("home_"):]
            if partner in columns:
                pairs.append((col, partner))
                seen.update([col, partner])

        elif col.startswith("away_"):
            partner = "home_" + col[len("away_"):]
            if partner in columns:
                pairs.append((col, partner))
                seen.update([col, partner])

    return pairs


def augment_home_away(
    df: pd.DataFrame,
    target_reg: str = "spread_home",
    target_cls: str = "home_win",
) -> pd.DataFrame:
    """Double the dataset by appending rows with home/away teams swapped.

    For each original row a mirror is created where:
      * All paired home_/away_ feature columns are swapped
      * ``spread_home`` is negated
      * ``home_win`` is flipped (1 → 0, 0 → 1)
      * ``neutral_site`` stays the same

    Only call this on the **training** split — validation data should stay
    unaugmented so metrics reflect real-world performance.
    """
    feature_cols = [c for c in df.columns if c not in (target_reg, target_cls)]
    pairs = _build_swap_pairs(feature_cols)

    flipped = df.copy()

    # Swap paired feature columns (read from original to avoid overwrites)
    for col_a, col_b in pairs:
        flipped[col_a] = df[col_b].values
        flipped[col_b] = df[col_a].values

    # Negate regression target and flip classification target
    if target_reg in flipped.columns:
        flipped[target_reg] = -df[target_reg].values
    if target_cls in flipped.columns:
        flipped[target_cls] = (1 - df[target_cls]).values

    augmented = pd.concat([df, flipped], ignore_index=True)
    print(f"↕ Augmented {len(df):,} → {len(augmented):,} rows (home/away swap)")
    return augmented
