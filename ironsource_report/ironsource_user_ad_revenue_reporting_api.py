import logging
import pandas as pd
import requests
from pandas import DataFrame

from applovin_report.utils.datetime_utils import day_ago
from applovin_report.utils.logging_utils import logging_basic_config
from
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

    ENDPOINT = "https://platform.ironsrc.com/partners/adRevenueMeasurements/v3"

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

    def get_report(
            self,
            date: str = day_ago(1),
            app_key: str = "",
            **kwargs
    ) -> DataFrame:
        """
        Retrieve a report from the ironSource Impression Level Revenue Server-Side API.

        Args:
            date: YYYY-MM-DD (UTC Timezone)
            app_key: Application Key (as seen on our platform)
            **kwargs: Additional parameters to pass to the API

        Returns:
            A pandas DataFrame containing the report data.

        Doc Author:
            mungvt@ikameglobal.com
        """

        params = {
            "appKey": app_key,
            "date": date,
            **kwargs,
        }

        headers = {
            'Authorization': f'Bearer {self.api_key}'
        }

        response = self.session.get(url=self.ENDPOINT, params=params, headers=headers)
        if response.status_code == 404:
            logging.warning(response.text + '. Skipped it.')
            return pd.DataFrame()
        else:
            report_file_urls = response.json()['urls']
            logging.info('Found {} report file(s)'.format(len(report_file_urls)))
            report_dfs = list(map(self._handle_report_file, report_file_urls))
            # Concat DFs
            result = pd.concat(report_dfs).reset_index().drop(columns=['index'])
            return result

    @staticmethod
    def _handle_report_file(url):
        result = pd.read_csv(url, compression='gzip', dtype={
            'advertising_id': str,
            'ad_network': str,
            'revenue': str
        })  # Read report to DF

        if result.empty:
            logging.warning(f"Not found data in report file at url: {url}.")
        else:
            logging.info(f"Collected successful ad revenue report file at url: {url}.")
        return result
