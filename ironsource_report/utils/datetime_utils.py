from datetime import datetime, timedelta


def day_ago(num_day=1):
    return (datetime.now() - timedelta(days=num_day)).strftime("%Y-%m-%d")
