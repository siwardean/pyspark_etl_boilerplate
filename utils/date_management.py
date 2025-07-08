from datetime import datetime, timedelta

def add_days(date_str, days, fmt="%Y-%m-%d"):
    date = datetime.strptime(date_str, fmt)
    return (date + timedelta(days=days)).strftime(fmt)
