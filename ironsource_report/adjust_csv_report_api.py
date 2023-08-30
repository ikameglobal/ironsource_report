import io
import logging

import pandas as pd
from pandas import DataFrame

from ironsource_report.utils.logging_utils import logging_basic_config
from ironsource_report.adjust_api import AdjustClient

logging_basic_config()
STATUS_RETRIES = (500, 502, 503, 504)


class CSVReport(AdjustClient):
    """
    Detailed documentation for this API can be found at:
        [CSV Report API](
        https://dash.adjust.com/control-center/reports-service/csv_report
        )
    """

    def __init__(self, api_key: str,
                 status_retries: list[int] = STATUS_RETRIES,
                 max_retries=5, retry_delay=1,
                 mute_log: bool = False):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.ERROR if mute_log else logging.INFO)
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
        super().__init__(api_key=api_key, status_retries=status_retries, max_retries=max_retries,
                         retry_delay=retry_delay)

    def get_report(
        self,
        params: dict = None,
        **kwargs
    ) -> DataFrame:
        """
        Retrieve a report from the Adjust Server-Side API.

        Args:
            params: params to pass to the API
            **kwargs: Additional parameters to pass to the API

        Returns:
            A pandas DataFrame containing the report data.

        Doc Author:
            mungvt@ikameglobal.com
        """
        response = self.session.get(url=self.API_CSV_REPORT, params=params, headers=self._api_headers)
        if response.status_code == 200:
            adjust_report_df = pd.read_csv(io.StringIO(response.text))
            self.logger.info(f'Found report has {len(adjust_report_df)} rows.')
            return adjust_report_df
        elif response.status_code == 204:
            self.logger.warning(f'No content.')
            return pd.DataFrame()
        else:
            self.logger.warning(f'[{response.status_code}] Not found report due to : {response.json()}. Skipped it.')
            return pd.DataFrame()
