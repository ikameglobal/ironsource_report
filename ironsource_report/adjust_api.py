import requests

from ironsource_report.utils.logging_utils import logging_basic_config
from requests.adapters import HTTPAdapter, Retry

logging_basic_config()
STATUS_RETRIES = (500, 502, 503, 504)


class AdjustClient:
    """
    Detailed documentation for this API can be found at:
        [Adjust API](
        https://help.adjust.com/en/article/reports-endpoint
        )
    """
    BASE_URL = 'https://dash.adjust.com/'
    API_CSV_REPORT = f"{BASE_URL}/control-center/reports-service/csv_report"

    def __init__(self, api_key: str,
                 status_retries: list[int] = STATUS_RETRIES,
                 max_retries=5, retry_delay=1):
        """
        Args:
            api_key: API key to use for the report
            status_retries: A set of HTTP status codes that we should force a retry on
            max_retries: Total number of retries to allow
            retry_delay: Num of seconds sleep between attempts

        Returns:
            None

        Doc Author:
            mungvt@ikameglobal.com
        """
        self._api_headers = {'Authorization': f'Bearer {api_key}'}
        self.session = requests.Session()
        retries = Retry(total=max_retries, backoff_factor=retry_delay, status_forcelist=status_retries)
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
