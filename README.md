# A Python library to download preprocessed data from the ESIOS API (REE)

ESIOS API is a service provided by the Spanish electricity system operator (REE) that offers access to a wide range of data related to the electricity market in Spain.

This library provides a simple interface to download and preprocess the data from the ESIOS API.

## Install library

```shell
pip install python-esios
```

## Get token

Ask for a personal token to access the ESIOS API following the [instructions from REE](https://www.esios.ree.es/es/pagina/api).

## Usage

### Register the token in Python

```python
TOKEN = '343sdfewe342309gjarijgwoiret834383434524...'
TOKEN = '<YOUR_TOKEN>'
```

Then, set the token in the environment variable `ESIOS_API_KEY`.

```python
import os
os.environ['ESIOS_API_KEY'] = TOKEN
```

### Instantiate the client

```python
from esios import ESIOSClient
client = ESIOSClient()
```

### Access the endpoint

```python
endpoint = client.endpoint(name=?)
```

In the tutorials below, you will learn how to download, preprocess, and visualize the data from the following endpoints:

- [Indicators](https://github.com/datons/python-esios/blob/main/examples/30_Indicators/0_Steps/B1_Download.ipynb)
- [Archives](https://github.com/datons/python-esios/blob/main/examples/20_Archives/0_Steps/B1_Download.ipynb)