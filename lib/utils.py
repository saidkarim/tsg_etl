# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from typing import Generator


def last_weeks_day() -> Generator[str, None, None]:
    """Generate list of last week's days"""
    now = datetime.now()
    start_of_last_week = now - timedelta(days=now.weekday() + 7)

    for i in range(7):
        yield (start_of_last_week + timedelta(days=i)).strftime("%Y-%m-%d")
