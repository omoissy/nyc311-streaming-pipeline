from __future__ import annotations

from datetime import UTC, datetime

import pytest

from nyc311_stream.transform import SCHEMA_VERSION, InvalidRecord, format_request

NOW = datetime(2026, 11, 11, 12, 0, tzinfo=UTC)


def test_format_request_normalizes_real_source_fields(raw_request):
    result = format_request(raw_request, now=NOW)

    assert result == {
        "schema_version": SCHEMA_VERSION,
        "unique_key": "70000001",
        "created_date": "2026-11-10T10:00:00.000Z",
        "closed_date": "2026-11-10T14:30:00.000Z",
        "agency": "DOT",
        "agency_name": "Department of Transportation",
        "complaint_type": "Street Condition",
        "descriptor": "Pothole",
        "location_type": "Street/Sidewalk",
        "incident_zip": "10001",
        "city": "New York",
        "borough": "MANHATTAN",
        "status": "Closed",
        "due_date": "2026-11-12T10:00:00.000Z",
        "resolution_description": "The condition was repaired.",
        "latitude": 40.7505,
        "longitude": -73.9972,
        "community_board": "05",
        "council_district": "03",
        "event_time": "2026-11-10T10:00:00.000Z",
    }


def test_optional_fields_are_null(raw_request):
    for key in ("closed_date", "due_date", "resolution_description", "latitude", "longitude"):
        raw_request.pop(key, None)

    result = format_request(raw_request)

    assert result["closed_date"] is None
    assert result["due_date"] is None
    assert result["latitude"] is None
    assert result["longitude"] is None


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda record: record.pop("unique_key"), "unique_key"),
        (lambda record: record.update(created_date="bad"), "created_date"),
        (lambda record: record.update(latitude="not-a-number"), "latitude"),
        (lambda record: record.pop("complaint_type"), "complaint_type"),
    ],
)
def test_invalid_source_rows_are_rejected(raw_request, mutate, message):
    mutate(raw_request)

    with pytest.raises(InvalidRecord, match=message):
        format_request(raw_request)
