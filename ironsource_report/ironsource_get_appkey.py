import logging
import pandas as pd
from pandas import DataFrame

from ironsource_report.utils.logging_utils import logging_basic_config
from ironsource_report.ironsource_api import IronSourceClient

logging_basic_config()
STATUS_RETRIES = (500, 502, 503, 504)


class ApplicationAPI(IronSourceClient):
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

    def get_app_keys(
        self,
        platform: str = None,
        app_status: str = None,
        **kwargs
    ) -> DataFrame:
        """
        Use this API to retrieve a list of all of your apps. Detailed documentation for this API can be found at:
        [IronSource Application API](
        "https://developers.is.com/ironsource-mobile/air/application-api/#step-1"
        )

        Args:
            platform: Operating system	ios / android
            app_status: Application activation status	Active / archived
            **kwargs: Additional parameters to pass to the API

        Returns:
            A pandas DataFrame with columns: appKey, platform, app_package_name

        Doc Author:
            mungvt@ikameglobal.com
        """

        params = {}
        if platform:
            params['platform'] = platform
        if app_status:
            params['appStatus'] = app_status
        if kwargs:
            params.update(kwargs)

        response = self.session.get(url=self.API_APPLICATION, params=params, headers=self._api_headers)
        if response.status_code == 404:
            logging.warning(response.text + '. Skipped it.')
            return pd.DataFrame()
        else:
            app_keys = []
            for app in response.json():
                if app['platform'].lower() == 'ios':
                    app_package_name = app['trackId']
                elif app['platform'].lower() == 'android':
                    app_package_name = app['bundleId']
                else:
                    # Skip apps/games with no app_package_name
                    continue

                app_dict = {
                    'app_key': app['appKey'] if 'appKey' in app else '',
                    'platform': app['platform'] if 'platform' in app else '',
                    'app_package_name': app_package_name
                }
                app_keys.append(app_dict)
            logging.info('Found {} app key(s)'.format(len(app_keys)))
            app_keys_df = pd.DataFrame(app_keys)
            app_keys_df.dropna(inplace=True)
            return app_keys_df
