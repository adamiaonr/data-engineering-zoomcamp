# Week 1 notes

## Parse datetime strings as `pd.datetime` directly in `pd.read_csv()`

Instead of changing columns to `pd.datetime` after calling `pd.read_csv()`, we can do it directly in the function call by passing a `parse_dates` argument:

```python
rides = pd.read_csv(
    Path('green_tripdata_2019-01.csv'), 
    parse_dates=['lpep_pickup_datetime', 'lpep_dropoff_datetime']
)
```

## Ingest a Pandas dataframe in chunks to a SQL database without iterators

If your goal is to partition the ingestion of data in a SQL database (and not the reading of the `.csv` file into memory), you can do it directly in the `pd.DataFrame.to_sql()` function, e.g.:

```python
import pandas as pd

from sqlalchemy import create_engine

HOST=...
PORT=...

USER=...
PWD=...
DB_NAME=...

engine=create_engine(f'postgresql://{USER}:{PWD}@{HOST}:{PORT}/{DB_NAME}')

rides = pd.read_csv(
    Path('green_tripdata_2019-01.csv'), 
    parse_dates=['lpep_pickup_datetime', 'lpep_dropoff_datetime']
)

rides.to_sql(name='rides', con=engine, index=False, if_exists='append', chunksize=10000)
```

## Docker `--network="host"` option to access `localhost` from within containers

As mentioned in [this stackoverflow answer](https://stackoverflow.com/a/24326540/6660861), we can access the host's network - and thus `localhost` - from within a container by passing `--network="host"` to the `docker run` command, e.g.:

```
docker run -it --network="host" --entrypoint=bash ingest:latest
```
