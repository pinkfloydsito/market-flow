from datetime import datetime, timedelta


def get_start_end_dates(year, month):
    """
    Get the start and end dates for a given year and month.
    """
    start_date = datetime(year, month, 1)

    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)

    return start_date, end_date
