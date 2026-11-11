from __future__ import annotations

import pandas as pd
import pytest
from metrics import (
    data_quality,
    hourly_volume,
    prepare,
    recent_cutoff,
    resolution_times,
    top_counts,
)


def request(created, closed=None, complaint="Noise", status="Closed", borough="BROOKLYN"):
    return {
        "unique_key": f"{created}-{complaint}",
        "created_date": pd.Timestamp(created),  # Cassandra returns naive UTC datetimes
        "closed_date": pd.Timestamp(closed) if closed else None,
        "complaint_type": complaint,
        "status": status,
        "borough": borough,
        "agency": "NYPD",
    }


def test_timestamps_become_new_york_time():
    data = prepare(pd.DataFrame([request("2026-11-10 15:00"), request("2026-07-04 16:00")]))

    assert str(data["created_date"].dt.tz) == "America/New_York"
    assert data["created_date"].dt.hour.tolist() == [10, 12]  # EST, then EDT


def test_missing_categories_are_labelled_unknown():
    data = prepare(pd.DataFrame([request("2026-11-10 15:00", borough=None, status=None)]))
    assert data.loc[0, "borough"] == "Unknown"
    assert data.loc[0, "status"] == "Unknown"


def test_hourly_volume_counts_by_local_hour_and_fills_gaps():
    data = prepare(
        pd.DataFrame(
            [
                request("2026-11-10 15:05"),
                request("2026-11-10 15:55"),
                request("2026-11-10 17:10"),
            ]
        )
    )

    volume = hourly_volume(data)

    assert volume["requests"].tolist() == [2, 0, 1]
    assert volume["hour"].dt.hour.tolist() == [10, 11, 12]


def test_top_counts_are_largest_first_and_limited():
    data = pd.DataFrame({"complaint_type": ["A", "B", "B", "C", "C", "C"]})

    result = top_counts(data, "complaint_type", limit=2)

    assert result[["complaint_type", "requests", "label"]].to_dict("records") == [
        {"complaint_type": "C", "requests": 3, "label": "50%"},
        {"complaint_type": "B", "requests": 2, "label": "33%"},
    ]


def test_resolution_times_use_closed_requests_only():
    rows = [request("2026-11-10 10:00", f"2026-11-10 {10 + h}:00", "Noise") for h in (1, 2, 3, 4)]
    rows += [request("2026-11-10 10:00", None, "Noise", status="Open")]
    rows += [request("2026-11-10 10:00", "2026-11-10 09:00", "Noise")]  # closed before created
    rows += [request("2026-11-10 10:00", "2026-11-11 10:00", "Heat")] * 3

    result = resolution_times(prepare(pd.DataFrame(rows)), min_closed=3)

    assert result["complaint_type"].tolist() == ["Noise", "Heat"]  # fastest first
    noise = result.iloc[0]
    assert noise["closed"] == 4
    assert noise["median_hours"] == pytest.approx(2.5)
    assert (noise["p25"], noise["p75"]) == (pytest.approx(1.75), pytest.approx(3.25))
    assert result.iloc[1]["median_hours"] == pytest.approx(24)


def test_resolution_times_drop_types_with_too_few_closures():
    rows = [request("2026-11-10 10:00", "2026-11-10 12:00", "Rare")] * 5
    assert resolution_times(prepare(pd.DataFrame(rows)), min_closed=20).empty


def test_data_quality_counts_what_charts_exclude():
    rows = [
        request("2026-11-10 10:00", "2026-11-10 12:00"),
        request("2026-11-10 10:00", None, status="Open"),
        request("2026-11-10 10:00", "2026-11-10 08:00"),
    ]

    assert data_quality(prepare(pd.DataFrame(rows))) == {
        "rows": 3,
        "closed": 2,
        "open": 1,
        "closed_before_created": 1,
    }


def test_empty_inputs_are_handled():
    empty = prepare(pd.DataFrame(columns=["created_date", "closed_date", "complaint_type"]))
    assert hourly_volume(empty).empty
    assert data_quality(empty)["rows"] == 0


def test_top_counts_share_is_of_all_rows_not_just_the_top():
    data = pd.DataFrame({"status": ["Closed"] * 997 + ["Open"] * 2 + ["Pending"]})

    result = top_counts(data, "status", limit=2)

    assert result["share"].round(3).tolist() == [0.997, 0.002]
    assert result["label"].tolist() == ["100%", "<1%"]


def test_recent_cutoff_marks_the_trailing_hours():
    data = prepare(pd.DataFrame([request("2026-11-10 06:58"), request("2026-11-09 12:00")]))

    cutoff = recent_cutoff(data, hours=6)

    # Latest is 01:58 New York time; the six trailing hours are 20:00 through 01:59.
    assert cutoff == pd.Timestamp("2026-11-09 20:00", tz="America/New_York")
