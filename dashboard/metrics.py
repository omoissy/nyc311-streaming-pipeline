"""Data preparation for the dashboard, kept free of Streamlit so it can be unit tested.

Cassandra stores timestamps in UTC; NYC readers think in New York time, so every chart
groups and labels by America/New_York.
"""

from __future__ import annotations

import pandas as pd

NYC_TZ = "America/New_York"
TIMESTAMP_COLUMNS = ("created_date", "closed_date")
# NYC publishes recent requests in batches, so the newest hours are often partial.
RECENT_HOURS_MAY_BE_INCOMPLETE = 6


def prepare(rows: pd.DataFrame) -> pd.DataFrame:
    """Parse UTC timestamps into New York time and label missing categories."""
    data = rows.copy()
    for column in TIMESTAMP_COLUMNS:
        if column in data:
            data[column] = pd.to_datetime(data[column], utc=True).dt.tz_convert(NYC_TZ)
    for column in ("borough", "complaint_type", "status", "agency"):
        if column in data:
            data[column] = data[column].fillna("Unknown")
    return data


def hourly_volume(data: pd.DataFrame) -> pd.DataFrame:
    """Requests per New York hour of `created_date`, with empty hours shown as zero."""
    if data.empty:
        return pd.DataFrame(columns=["hour", "requests"])
    counts = data.set_index("created_date").resample("1h").size()
    return counts.rename("requests").rename_axis("hour").reset_index()


def recent_cutoff(data: pd.DataFrame, hours: int = RECENT_HOURS_MAY_BE_INCOMPLETE):
    """Start of the trailing period whose counts may still be filling in."""
    return data["created_date"].max().floor("1h") - pd.Timedelta(hours=hours - 1)


def top_counts(data: pd.DataFrame, column: str, limit: int | None = None) -> pd.DataFrame:
    """Row counts per category, largest first, with each category's share of all rows."""
    counts = data[column].value_counts()
    total = int(counts.sum())
    if limit:
        counts = counts.head(limit)
    result = counts.rename("requests").rename_axis(column).reset_index()
    result["share"] = result["requests"] / total if total else 0.0
    result["label"] = [
        "<1%" if 0 < share < 0.005 else f"{share:.0%}" for share in result["share"]
    ]
    return result


def resolution_times(data: pd.DataFrame, top: int = 10, min_closed: int = 20) -> pd.DataFrame:
    """Median and interquartile hours from creation to closure, per complaint type.

    Only closed requests have a duration, and the fastest closures arrive first, so
    medians computed over a recent window lean low. Types with fewer than `min_closed`
    closures are dropped as too noisy. Rows closing before they were created are data
    errors and are excluded.
    """
    hours = (data["closed_date"] - data["created_date"]).dt.total_seconds() / 3600
    closed = data.assign(hours=hours)[hours.notna() & (hours >= 0)]
    if closed.empty:
        return pd.DataFrame(columns=["complaint_type", "closed", "median_hours", "p25", "p75"])
    stats = (
        closed.groupby("complaint_type")["hours"]
        .agg(
            closed="size",
            median_hours="median",
            p25=lambda s: s.quantile(0.25),
            p75=lambda s: s.quantile(0.75),
        )
        .reset_index()
    )
    stats = stats[stats["closed"] >= min_closed]
    return stats.nlargest(top, "closed").sort_values("median_hours").reset_index(drop=True)


def data_quality(data: pd.DataFrame) -> dict[str, int]:
    """Counts that explain what the charts include and leave out."""
    if data.empty:
        return {"rows": 0, "closed": 0, "open": 0, "closed_before_created": 0}
    hours = (data["closed_date"] - data["created_date"]).dt.total_seconds()
    return {
        "rows": len(data),
        "closed": int(data["closed_date"].notna().sum()),
        "open": int(data["closed_date"].isna().sum()),
        "closed_before_created": int((hours < 0).sum()),
    }
