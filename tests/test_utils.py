import pytest
from lib.utils import last_weeks_day
from datetime import datetime


@pytest.fixture
def mock_datetime_now(monkeypatch):

    class MockDateTime:
        @classmethod
        def now(cls):
            return datetime(2023, 11, 15)

    monkeypatch.setattr("lib.utils.datetime", MockDateTime)


def test_last_weeks_day(mock_datetime_now):
    expected_dates = [
        "2023-11-06",
        "2023-11-07",
        "2023-11-08",
        "2023-11-09",
        "2023-11-10",
        "2023-11-11",
        "2023-11-12",
    ]
    generated_dates = list(last_weeks_day())

    assert (
        generated_dates == expected_dates
    ), "The generated dates do not match the expected ones."
