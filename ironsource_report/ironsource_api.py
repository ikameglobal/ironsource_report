import requests

from ironsource_report.utils.logging_utils import logging_basic_config
from requests.adapters import HTTPAdapter, Retry

logging_basic_config()
STATUS_RETRIES = (500, 502, 503, 504)


class IronSourceClient:
    """
    Detailed documentation for this API can be found at:
        [ironSource Impression Level API](
        https://developers.is.com/ironsource-mobile/air/ad-revenue-measurements/#step-1
        )
    """
    BASE_URL = 'https://platform.ironsrc.com'
    API_AUTH = f'{BASE_URL}/partners/publisher/auth'
    API_APPLICATION = f"{BASE_URL}/partners/publisher/applications/v6?"
    API_AD_REVENUE = f"{BASE_URL}/partners/adRevenueMeasurements/v3"

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
        self._api_credential = api_credential
        self._api_key = self.__get_bearer_token()
        self._api_headers = {'Authorization': f'Bearer {self._api_key}'}

        self.session = requests.Session()
        retries = Retry(total=max_retries, backoff_factor=retry_delay, status_forcelist=status_retries)
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    def __get_bearer_token(self):
        """
            Get bearer token for ironSource apps. Requires "secretkey" and "refreshToken". Returns a Bearer token.
        """
        auth_headers = {
            "secretkey": self._api_credential['secretkey'],
            "refreshToken": self._api_credential['refreshToken']
        }
        # Call the auth API to get Bearer token
        response = requests.get(self.API_AUTH, headers=auth_headers)
        bearer_token = response.text.replace('"', '')
        return bearer_token
