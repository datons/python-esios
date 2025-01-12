import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

class OfferIndicators:
    def __init__(self, client):
        self.client = client

    def _html_to_text(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        return '\n\n'.join([p.get_text() for p in soup.find_all('p')])

    def list(self):
        endpoint = "offer_indicators"
        data = self.client._get(endpoint, self.client.public_headers)
        
        indicators = data.get('indicators', [])
        for indicator in indicators:
            indicator['description'] = self._html_to_text(indicator['description'])
            
        return pd.DataFrame(indicators)

    def select(self, id):
        return OfferIndicator(self.client, id)

class OfferIndicator:
    def __init__(self, client, id):
        self.client = client
        self.id = id
        self.metadata = self._get_metadata()

    def _get_metadata(self):
        endpoint = f"offer_indicators/{self.id}"
        data = self.client._get(endpoint, self.client.public_headers).get('indicator', {})
        data.pop('values')
        return data

    def historical(self, start=None, end=None, locale='es'):
        params = {
            'start_date': start,
            'end_date': end,
            'locale': locale
        }
        
        # Remove None values from params
        params = {k: v for k, v in params.items() if v is not None}
        
        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d')
        three_weeks = timedelta(days=3)

        data_all = []

        current_start = start_date
        while current_start < end_date:
            current_end = min(current_start + three_weeks, end_date)
            chunk_params = params.copy()
            chunk_params['start_date'] = current_start.strftime('%Y-%m-%d')
            chunk_params['end_date'] = current_end.strftime('%Y-%m-%d')

            endpoint = f"offer_indicators/{self.id}"
            data = self.client._get(endpoint, self.client.public_headers, params=chunk_params)
            data_all.extend(data.get('indicator', {}).get('values', []))

            current_start = current_end + timedelta(days=1)

        return self._to_dataframe(data_all)

    def _to_dataframe(self, data):
        if data:
            df = pd.DataFrame(data)
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
                df = df.set_index('datetime')
                df.index = df.index.tz_convert('Europe/Madrid')
            
            df = df[[col for col in df.columns if 'time' not in col]]
            
            return df
        else:
            return pd.DataFrame()

    def forecast(self):
        # Implement forecast functionality similar to historical
        pass
