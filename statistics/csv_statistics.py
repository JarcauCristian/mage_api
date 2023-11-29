import json
from typing import Dict, Any
import requests
import io
from statistics.client import Client
import pandas as pd


class CSVLoader:

    def __init__(self, path: str = ""):
        self.path = path
        self._endpoint = 'get_object'
        self._ctx = Client()
        self._df = pd.DataFrame()

    def set_params(self, path) -> None:
        self.path = path

    @property
    def init_params(self):
        return ["path"]

    def execute(self, token: str) -> None:
        try:
            response = self._ctx.send_request(endpoint=self._endpoint,
                                              method="GET",
                                              query_params={'dataset_path': self.path},
                                              headers={'Authorization': f'Bearer {token}'})
            response_url = json.loads(response.content.decode('utf-8'))["url"]
            try:
                response = requests.get(response_url)

                if response.status_code != 200:
                    print(f"Error fetching data from the server, status_code: {response.status_code}")

                self._df = pd.read_csv(io.StringIO(response.text))
            except Exception as e:
                print(f"Error fetching data from the server: {str(e)}")

        except TypeError as e:
            print(f"An error occurred: {e}")

    def get_statistics(self) -> Dict[str, Any]:
        columns = list(self._df.describe().columns)

        return {"columns_dataset": columns,
                "df": self._df.describe().T.to_dict(orient='record')}