import pandas as pd
from datetime import datetime, timedelta
from .client import BaseData, ESIOSClient

class IndicatorData(BaseData):
    def __init__(self, client: ESIOSClient, indicator_id: str):
        super().__init__(client)
        self.indicator_id = indicator_id
        self.data = None
        self.metadata = None
        self.historical_params = None
        self.historical_data = None
        self.forecast_params = None
        self.forecast_data = None

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
        
        # Check if the requested params are different from the cached ones
        if params != self.historical_params:
            start_date = datetime.strptime(start, '%Y-%m-%d')
            end_date = datetime.strptime(end, '%Y-%m-%d')
            three_weeks = timedelta(weeks=3)

            all_data = []

            current_start = start_date
            while current_start < end_date:
                current_end = min(current_start + three_weeks, end_date)
                chunk_params = params.copy()
                chunk_params['start_date'] = current_start.strftime('%Y-%m-%d')
                chunk_params['end_date'] = current_end.strftime('%Y-%m-%d') + 'T23:59:59'

                endpoint = f"{self.client.public_base_url}/indicators/{self.indicator_id}"
                response = self.get(endpoint, self.client.public_headers, params=chunk_params)
                data_chunk = response.json()
                all_data.extend(data_chunk.get('indicator', {}).get('values', []))

                current_start = current_end + timedelta(days=1)

            self.historical_data = {'indicator': {'values': all_data}}
            self.historical_params = params
            self.metadata = self._get_metadata(self.historical_data)

        return self._to_dataframe(column_name)

    def _to_dataframe(self, column_name='value'):
        data = self.historical_data.get('indicator', {})
        values = data.get('values', [])
        
        if values:
            df = pd.DataFrame(values)
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
                df = df.set_index('datetime')
                df.index = df.index.tz_convert('Europe/Madrid')
            
            df = df[[col for col in df.columns if 'time' not in col]]
            
            if column_name in data and column_name != 'value':
                df.rename(columns={'value': data[column_name]}, inplace=True)
            
            return df
        else:
            return pd.DataFrame()

    def _get_metadata(self, data):
        metadata = data.get('indicator', {}).copy()
        metadata.pop('values', None)
        return metadata
