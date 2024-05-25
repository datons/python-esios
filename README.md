# A Python library to download preprocessed data from the ESIOS API (REE)

ESIOS API is a service provided by the Spanish electricity system operator (REE) that offers access to a wide range of data related to the electricity market in Spain.

This library provides a simple interface to download and preprocess the data from the ESIOS API.

## Install library

```shell
pip install python-esios
```

## Usage

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

- [Indicators](https://github.com/datons/python-esios/blob/main/examples/20_Indicators/0_Steps.ipynb)
- [Archives](https://github.com/datons/python-esios/blob/main/examples/30_Archives/0_Steps.ipynb)