from __future__ import annotations

from datetime import UTC, datetime

import pytest

from user_stream.transform import SCHEMA_VERSION, InvalidRecord, format_user

NOW = datetime(2026, 11, 11, 12, 0, 0, tzinfo=UTC)


def test_flattens_a_full_record(raw_user):
    record = format_user(raw_user, now=NOW)

    assert record == {
        "schema_version": SCHEMA_VERSION,
        "id": "44957db7-9c04-4cff-8020-85aa39e37b4b",
        "first_name": "Emma",
        "last_name": "Johansen",
        "gender": "female",
        "address": "4587 Teglværksvej, Rønnede, Midtjylland, Denmark",
        "post_code": "63718",
        "email": "emma.johansen@example.com",
        "username": "beautifulleopard431",
        "dob": "1982-06-24T23:39:34.775Z",
        "registered_date": "2015-04-07T18:30:40.908Z",
        "phone": "58147274",
        "picture": "https://randomuser.me/api/portraits/med/women/20.jpg",
        "nationality": "DK",
        "event_time": "2026-11-11T12:00:00.000+00:00",
    }


def test_string_postcode_is_kept(raw_users):
    assert format_user(raw_users[1], now=NOW)["post_code"] == "GE78 5SE"


def test_uuid_is_normalised(raw_user):
    raw_user["login"]["uuid"] = "44957DB79C044CFF802085AA39E37B4B"
    assert format_user(raw_user, now=NOW)["id"] == "44957db7-9c04-4cff-8020-85aa39e37b4b"


def test_optional_fields_may_be_missing(raw_user):
    for key in ("location", "picture", "phone", "gender", "nat", "dob", "registered"):
        raw_user.pop(key)

    record = format_user(raw_user, now=NOW)

    assert record["address"] is None
    assert record["picture"] is None
    assert record["nationality"] is None
    assert record["dob"] is None
    assert record["first_name"] == "Emma"


def test_partial_address_skips_missing_parts(raw_user):
    del raw_user["location"]["street"]
    del raw_user["location"]["state"]
    assert format_user(raw_user, now=NOW)["address"] == "Rønnede, Denmark"


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda r: r["login"].pop("uuid"), "login.uuid"),
        (lambda r: r["login"].update(uuid="not-a-uuid"), "not a valid UUID"),
        (lambda r: r.pop("email"), "email"),
        (lambda r: r.update(email="no-at-sign.example.com"), "email is not valid"),
        (lambda r: r["name"].update(first="   "), "name.first"),
        (lambda r: r.pop("name"), "name.first"),
        (lambda r: r.update(login="oops"), "login.uuid"),
    ],
)
def test_invalid_records_are_rejected(raw_user, mutate, message):
    mutate(raw_user)
    with pytest.raises(InvalidRecord, match=message):
        format_user(raw_user, now=NOW)


def test_non_object_record_is_rejected():
    with pytest.raises(InvalidRecord, match="not an object"):
        format_user(["not", "a", "dict"])  # type: ignore[arg-type]


def test_event_time_defaults_to_now(raw_user):
    record = format_user(raw_user)
    parsed = datetime.fromisoformat(record["event_time"])
    assert abs((datetime.now(UTC) - parsed).total_seconds()) < 5
