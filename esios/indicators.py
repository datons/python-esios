import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

class Indicators:
    def __init__(self, client):
        self.client = client

    def _html_to_text(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        return '\n\n'.join([p.get_text() for p in soup.find_all('p')])

    def list(self):
        endpoint = "indicators"
        data = self.client._get(endpoint, self.client.public_headers)
        
        indicators = data.get('indicators', [])
        for indicator in indicators:
            indicator['description'] = self._html_to_text(indicator['description'])
        return pd.DataFrame(indicators)

    def select(self, id):
        return Indicator(self.client, id)

class Indicator:
    def __init__(self, client, id):
        self.client = client
        self.id = id
        self.metadata = self._get_metadata()

    def _get_metadata(self):
        endpoint = f"indicators/{self.id}"
        data = self.client._get(endpoint, self.client.public_headers)
        return data.get('indicator', {})

    def historical(self, start=None, end=None, geo_ids=None, locale='es', time_agg=None, geo_agg=None, time_trunc=None, geo_trunc=None, column_name='id'):
        params = {
            'start_date': start,
            'end_date': end + 'T23:59:59' if end else None,
            'geo_ids[]': ','.join(map(str, geo_ids)) if geo_ids else None,
            'locale': locale,
            'time_agg': time_agg,
            'geo_agg': geo_agg,
            'time_trunc': time_trunc,
            'geo_trunc': geo_trunc
        }
        
        # Remove None values from params
        params = {k: v for k, v in params.items() if v is not None}
        
        start_date = pd.to_datetime(start)
        end_date = pd.to_datetime(end)
        three_weeks = timedelta(weeks=3)
        
        endpoint = f"indicators/{self.id}"
        
        if end_date - start_date <= three_weeks:
            data = self.client._get(endpoint, self.client.public_headers, params=params)
            data = data.get('indicator', {}).get('values', [])
            
            return self._to_dataframe(data, column_name)
        
        data_all = []

        current_start = start_date
        while current_start < end_date:
            current_end = min(current_start + three_weeks, end_date)
            chunk_params = params.copy()
            chunk_params['start_date'] = current_start.strftime('%Y-%m-%d')
            chunk_params['end_date'] = current_end.strftime('%Y-%m-%d') + 'T23:59:59'

            data = self.client._get(endpoint, self.client.public_headers, params=chunk_params)
            data_all.extend(data.get('indicator', {}).get('values', []))

            current_start = current_end + timedelta(days=1)

        return self._to_dataframe(data_all, column_name)

    def _to_dataframe(self, data, column_name='value'):
        if data:
            df = pd.DataFrame(data)
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
                df = df.set_index('datetime')
                df.index = df.index.tz_convert('Europe/Madrid')
            
            df = df[[col for col in df.columns if 'time' not in col]]
            
            if column_name in self.metadata and column_name != 'value':
                column_name = str(self.metadata[column_name])
                df.rename(columns={'value': column_name}, inplace=True)
            
            return df
        else:
            return pd.DataFrame()

    def forecast(self):
        # Implement forecast functionality similar to historical
        pass
