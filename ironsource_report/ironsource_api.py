import logging
import pandas as pd
import requests
from pandas import DataFrame

from applovin_report.utils.datetime_utils import day_ago
from applovin_report.utils.logging_utils import logging_basic_config
from requests.adapters import HTTPAdapter, Retry

logging_basic_config()
STATUS_RETRIES = (500, 502, 503, 504)


class AdRevenueMeasurements:
    """
    Detailed documentation for this API can be found at:
        [ironSource Impression Level API](
        https://developers.is.com/ironsource-mobile/air/ad-revenue-measurements/#step-1
        )
    """

    def __init__(self, api_key: str | list[str],
                 status_retries: list[int] = STATUS_RETRIES,
                 max_retries=5, retry_delay=1):
        """
        Args:
            api_key: API key(s) to use for the report
            status_retries: A set of HTTP status codes that we should force a retry on
            max_retries: Total number of retries to allow
            retry_delay: Num of seconds sleep between attempts

        Returns:
            None

        Doc Author:
            mungvt@ikameglobal.com
        """
        self.api_key = api_key
        self.session = requests.Session()
        retries = Retry(total=max_retries, backoff_factor=retry_delay, status_forcelist=status_retries)
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
