import logging
from urllib.error import HTTPError

import pandas as pd
from pandas import DataFrame

from ironsource_report.utils.datetime_utils import day_ago
from ironsource_report.utils.logging_utils import logging_basic_config
from ironsource_report.ironsource_api import IronSourceClient

logging_basic_config()
STATUS_RETRIES = (500, 502, 503, 504)


class AdRevenueMeasurements(IronSourceClient):
    """
    Detailed documentation for this API can be found at:
        [ironSource Impression Level API](
        https://developers.is.com/ironsource-mobile/air/ad-revenue-measurements/#step-1
        )
    """

    def __init__(self, api_credential: dict,
                 status_retries: list[int] = STATUS_RETRIES,
                 max_retries=5, retry_delay=1):
        """
        Args:
            api_credential: API key(s) to use for the report
            status_retries: A set of HTTP status codes that we should force a retry on
            max_retries: Total number of retries to allow
            retry_delay: Num of seconds sleep between attempts

        Returns:
            None

        Doc Author:
            mungvt@ikameglobal.com
        """
        super().__init__(api_credential=api_credential, status_retries=status_retries, max_retries=max_retries,
                         retry_delay=retry_delay)

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

        response = self.session.get(url=self.API_AD_REVENUE, params=params, headers=self._api_headers)
        if response.status_code == 200:
            report_file_urls = response.json()['urls']
            logging.info('Found {} report file(s)'.format(len(report_file_urls)))
            report_dfs = list(filter(
                lambda df: not df.empty,
                map(self._handle_report_file, report_file_urls))
            )
            # Concat DFs
            result = pd.concat(report_dfs).reset_index().drop(columns=['index'])
            return result
        else:
            logging.warning(response.text + '. Skipped it.')
            return pd.DataFrame()

    @staticmethod
    def _handle_report_file(url: str):
        try:
            result = pd.read_csv(url, compression='gzip', dtype={
                'advertising_id': str,
                'ad_network': str,
                'revenue': str
            })  # Read report to DF
            if result.empty:
                logging.warning(f"Not found data in report file url.")
            else:
                logging.info(f"Collected successful ad revenue report file url.")
            return result
        except HTTPError as e:
            logging.warning(f"Can not read csv file from url cause: {e}")
            return pd.DataFrame()
