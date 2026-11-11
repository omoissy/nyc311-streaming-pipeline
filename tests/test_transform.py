from __future__ import annotations

import pytest

from nyc311_stream.transform import SCHEMA_VERSION, InvalidRecord, format_request


def test_format_request_normalizes_real_source_fields(raw_request):
    result = format_request(raw_request)

    assert result == {
        "schema_version": SCHEMA_VERSION,
        "unique_key": "70000001",
        # NYC wall-clock times (EST in November) converted to UTC.
        "created_date": "2026-11-10T15:00:00.000Z",
        "closed_date": "2026-11-10T19:30:00.000Z",
        "agency": "DOT",
        "agency_name": "Department of Transportation",
        "complaint_type": "Street Condition",
        "descriptor": "Pothole",
        "location_type": "Street/Sidewalk",
        "incident_zip": "10001",
        "city": "New York",
        "borough": "MANHATTAN",
        "status": "Closed",
        "due_date": "2026-11-12T15:00:00.000Z",
        "resolution_description": "The condition was repaired.",
        "latitude": 40.7505,
        "longitude": -73.9972,
        "community_board": "05",
        "council_district": "03",
        "source_updated_at": "2026-11-11T02:39:47.123Z",
        "event_time": "2026-11-10T15:00:00.000Z",
    }


def test_summer_timestamps_use_daylight_time(raw_request):
    raw_request["created_date"] = "2026-07-04T12:00:00.000"
    assert format_request(raw_request)["created_date"] == "2026-07-04T16:00:00.000Z"


def test_optional_fields_are_null(raw_request):
    for key in (
        "closed_date",
        "due_date",
        "resolution_description",
        "latitude",
        "longitude",
        ":updated_at",
    ):
        raw_request.pop(key, None)

    result = format_request(raw_request)

    assert result["closed_date"] is None
    assert result["due_date"] is None
    assert result["latitude"] is None
    assert result["longitude"] is None
    assert result["source_updated_at"] is None


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda record: record.pop("unique_key"), "unique_key"),
        (lambda record: record.update(created_date="bad"), "created_date"),
        (lambda record: record.update(closed_date="2026-13-01T00:00:00.000"), "closed_date"),
        (lambda record: record.update(latitude="not-a-number"), "latitude"),
        (lambda record: record.pop("complaint_type"), "complaint_type"),
        (lambda record: record.update(status="  "), "status"),
        (lambda record: record.update({":updated_at": "yesterday"}), ":updated_at"),
    ],
)
def test_invalid_source_rows_are_rejected(raw_request, mutate, message):
    mutate(raw_request)

    with pytest.raises(InvalidRecord, match=message):
        format_request(raw_request)


def test_non_object_record_is_rejected():
    with pytest.raises(InvalidRecord, match="not an object"):
        format_request(["not", "a", "record"])  # type: ignore[arg-type]
