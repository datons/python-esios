import pandas as pd
import glob

def liquicomun(path_home, name = 'C2_compodem_'):

    pattern = f'{path_home}/*{name}*'
    paths = glob.glob(pattern)

    dfs = []
    for path in paths:
        df = pd.read_csv(
            path, sep=';', skiprows=2,
            skipfooter=1, engine='python', header=None
        )
        
        dfs.append(df)
    
    return pd.concat(dfs).dropna(axis=1, how='all')