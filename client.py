import gzip
import json
import os
import pandas

from io import BytesIO
from jwt_builder import JWTBuilder
from requests import Session


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

API_ENDPOINT = 'https://api.appstoreconnect.apple.com'
API_VERSION = 'v1'


def read_schema(schema_name: str) -> list:
    schema_path = os.path.join(BASE_DIR, 'schemas', f'{schema_name}.json')

    with open(schema_path, 'r', encoding='utf-8') as fh:
        schema = fh.read()

    return json.loads(schema)


class AppstoreConnect:
    def __init__(self, key_id, key_file, issuer_id):
        self._token = JWTBuilder(key_id, key_file, issuer_id).build()
        self._key_id = key_id
        self._key_file = key_file
        self._issuer_id = issuer_id
        self._req_session = Session()
        self._req_session.headers.update({
            'Authorization': f'Bearer {self._token}'
        })

    # https://developer.apple.com/documentation/appstoreconnectapi/download_sales_and_trends_reports
    def sales_report(self, report_date: str, vendor_number: str, retry_cnt: int = 3):
        result = []
        api_url = '/'.join([API_ENDPOINT, API_VERSION, 'salesReports'])
        schema = read_schema('sales_report')
        columns = [row['name'] for row in schema]

        response = self._req_session.get(api_url, params={
            'filter[frequency]': 'DAILY',
            'filter[reportDate]': report_date,
            'filter[reportSubType]': 'SUMMARY',
            'filter[reportType]': 'SALES',
            'filter[vendorNumber]': vendor_number
        })
        print(response.status_code)

        if response.status_code == 200:
            data = gzip.decompress(response.content)

            data_csv = pandas.read_csv(BytesIO(data), sep='\t')
            del data

            data_json = data_csv.to_json(orient='values')
            data_json = json.loads(data_json)
            del data_csv

            for row in data_json:
                result.append(dict(zip(columns, row)))

            return result

        elif response.status_code == 401 and retry_cnt > 0:
            retry_cnt = retry_cnt - 1
            self._token = JWTBuilder(self._key_id, self._key_file, self._issuer_id).build()
            return self.sales_report(report_date, vendor_number, retry_cnt)
        else:
            print(response.text)

            return {}
