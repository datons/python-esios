import pandas as pd
from esios.api_client import APIClient
import html

class Indicators(APIClient):
    def __init__(self, api_key=None):
        super().__init__(api_key)
    
    def list(self, df=False):
        """
        Fetches a list of indicators.

        Parameters
        ----------
        params : dict, optional
            URL parameters to be sent with the request.

        Returns
        -------
        list
            A list of indicators.
        """
        endpoint = "/indicators"
        response = self._api_call('GET', endpoint)
        
        data = response.json()
        
        if df:
            df = pd.DataFrame(data['indicators'])
            df['description'] = df['description'].apply(html.unescape)
            return df.set_index('id')
        else:
            return data

    def get(self, indicator_id, start_date=None, end_date=None, geo_ids=None, locale=None, time_agg=None, geo_agg=None, time_trunc=None, geo_trunc=None):
        """
        Fetches disaggregated indicator data optionally filtered by a date range and geo_ids,
        grouped by geo_id and month, using specified aggregation settings. All parameters except
        the indicator_id are optional.

        Parameters
        ----------
        indicator_id : int
            The ID of the indicator to fetch data for.
        start_date : str, optional
            The start date of the data range in ISO 8601 format (YYYY-MM-DD).
        end_date : str, optional
            The end date of the data range in ISO 8601 format (YYYY-MM-DD).
        geo_ids : list of int, optional
            A list of geographic IDs to filter the data by.
        locale : str, optional, default 'es'
            Language for translations. Defaults to Spanish ('es'). Can be 'es' for Spanish or 'en' for English.
        time_agg : str, optional, default 'average'
            Specifies how to aggregate the data over time. Typical values are 'average', 'sum', etc.
        geo_agg : str, optional, default 'average'
            Specifies how to aggregate the data geographically. Typical values are 'average', 'sum', etc.
        time_trunc : str, optional
            Specifies how to truncate the data time series. Typical values are 'day', 'month', 'year', etc.
        geo_trunc : str, optional
            Specifies how to group data at the geolocalization level. Typical values are 'province', 'region', etc.

        Returns
        -------
        IndicatorData
            An instance of IndicatorData containing the fetched data.
        """
        
        params = {}
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date + 'T23:59:59'
        if geo_ids:
            params['geo_ids'] = ','.join(map(str, geo_ids))
        if locale:
            params['locale'] = locale
        if time_agg:
            params['time_agg'] = time_agg
        if geo_agg:
            params['geo_agg'] = geo_agg
        if time_trunc:
            params['time_trunc'] = time_trunc
        if geo_trunc:
            params['geo_trunc'] = geo_trunc

        endpoint = f"/indicators/{indicator_id}"
        response = self._api_call('GET', endpoint, params=params)
        return IndicatorData(response.json())

class IndicatorData:
    def __init__(self, data):
        self.data = data

    def to_dataframe(self, column_name='value'):
        """
        Converts the indicator values data to a Pandas DataFrame.

        Parameters
        -------
        column_name : {'value', 'id', 'short_name'}, default 'value'
            
        Returns
        -------
        pandas.DataFrame
            A DataFrame containing the indicator values.
        """
        data = self.data.get('indicator', {})
        values = data.get('values', [])
        
        if values:
            df = pd.DataFrame(values)
            df = df.set_index('datetime')
            df.index = pd.to_datetime(df.index, utc=True)
            
            columns = df.columns
            mask = columns.str.contains('time')
            
            df.drop(columns=columns[mask], inplace=True)
            
            if column_name != 'value':
                df = df.rename(columns={'value': data[column_name]})
            
            return df.tz_convert('Europe/Madrid')
        else:
            return pd.DataFrame()

    def get_metadata(self):
        """
        Extracts metadata from the indicator data.

        Returns
        -------
        dict
            A dictionary containing the metadata of the indicator.
        """
        
        # extract all inside indicator key, except values
        metadata = self.data.get('indicator', {})
        metadata.pop('values', None)
        
        return metadata
