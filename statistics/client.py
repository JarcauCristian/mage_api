import json
from typing import Any
import urllib3
from dotenv import load_dotenv
import os
import requests


class Client:

    def __init__(self):
        load_dotenv()
        self._api = os.getenv('API')
        self._allowed_methods = ["GET", "POST", "PUT"]
        self._allowed_status_codes = [code for code in range(200, 300)]
        self._http = urllib3.PoolManager()

    def send_request(self, endpoint: str, method: str, query_params: dict[Any, Any] = None, body: dict[Any, Any] = None,
                     headers: dict[Any, Any] = None) -> requests.Response:
        url = f'{self._api}/{endpoint}'
        if query_params is not None:
            url += '?'
            for k, v in query_params.items():
                url += f'{k}={v}'
        if method.upper() in self._allowed_methods:
            if body is not None:
                if headers is not None:
                    response = requests.request(method.upper(), url, data=json.dumps(body), headers=headers)
                else:
                    response = requests.request(method.upper(), url, data=json.dumps(body))
            else:
                if headers is not None:
                    response = requests.request(method.upper(), url, headers=headers)
                else:
                    response = requests.request(method.upper(), url)

            if response.status_code in self._allowed_status_codes:
                return response
            else:
                raise TypeError(f"Value of status_code {response.status_code} is not in {self._allowed_status_codes}")
        else:
            raise TypeError(f"Value of method {method} is not in {self._allowed_methods}")

    def send_urllib3_request(self,
                             endpoint: str,
                             method: str,
                             query_params: dict[Any, Any] = dict,
                             fields: dict[Any, Any] = dict,
                             headers: dict[Any, Any] = dict) -> str | None:
        url = f'{self._api}/{endpoint}'
        if len(query_params) > 0:
            url += '?'
            for k, v in query_params.items():
                url += f'{k}={v}'
        if method.upper() in self._allowed_methods:
            if len(fields) > 0:
                if len(headers) > 0:
                    r = self._http.request(method, url, fields=fields, headers=headers)
                else:
                    r = self._http.request(method, url, fields=fields)
            else:
                if len(headers) > 0:
                    r = self._http.request(method, url, headers=headers)
                else:
                    r = self._http.request(method, url)

            if r.status in self._allowed_status_codes:
                return r.data.decode('utf-8')
            else:
                raise TypeError(f"Value of status_code {r.status} is not in {self._allowed_status_codes}")
        else:
            raise TypeError(f"Value of method {method} is not in {self._allowed_methods}")
