# HW6

## Overview

* Used Python
* Instead of a local Kafka deployment, decided to use [Confluent Cloud](https://confluent.cloud/home)
* My `consumer.py` is a bit 'dumb', as it prints the top 5 most popular location IDs by continuously updating a `pandas` dataframe. This should be done some other way (e.g., with `PySpark`, dataframes are updated automatically).

![image](https://user-images.githubusercontent.com/5468601/226191633-f1d721cd-81b6-41f2-8e67-1dd63e91b5ac.png)

## Required setup

1. Download the required `*.csv.gz` files into `data/fhv` and `data/green`
2. Initialize the virtual environment on machine 1 via `poetry` (install it if you don't have it already):
```
$ pip install --upgrade poetry
$ cd <repository>
$ poetry shell
```
3. Start a cluster in Confluent Cloud, add 3 topics: [`rides_green`, `rides_fhv`, `rides_all`].
4. Get an API key from Confluent Cloud for your cluster, add them to your environment:
```
export CLUSTER_API_KEY="<key>"
export CLUSTER_API_SECRET="<secret>"
export BOOTSTRAP_SERVER="<confluent-cloud-bootstrap-server-url>:9092"
```
5. On machine 2 - e.g., a GCP VM as recommended by the course - ensure you have PySpark ready to go (follow [the setup suggested for week 5](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/setup/linux.md)).

## Usage

1. Start `producer.py`:

```
$ python producer.py
```

After the script reads the `*.csv.gz` files, it should start producing data, interleaving messages to the 'green' and 'fhv' topics:

```
$ python producer.py
Record 264 successfully produced at offset rides_green.59
Record 140 successfully produced at offset rides_fhv.110
Record 97 successfully produced at offset rides_green.60
Record 141 successfully produced at offset rides_fhv.121
...
```

2. Start the aggregator task in Spark:

```
$ ./spark-submit.sh aggregator.py
```

After the initialization, you should start seeing the continuous stream processing 'in batches'.

```
-------------------------------------------
Batch: 0
-------------------------------------------
+--------------+--------------+-------------------+-------------------+-----+
|pu_location_id|do_location_id|pu_datetime        |do_datetime        |color|
+--------------+--------------+-------------------+-------------------+-----+
|140           |52            |2019-01-01 00:33:03|2019-01-01 01:37:24|fhv  |
|264           |264           |2018-12-21 15:17:29|2018-12-21 15:18:57|green|
+--------------+--------------+-------------------+-------------------+-----+

...

-------------------------------------------
Batch: 0
-------------------------------------------
+--------------+-----+
|pu_location_id|count|
+--------------+-----+
|264           |1    |
|140           |1    |
+--------------+-----+
```

3. Go to Confluent Cloud to check messages being published to the `rides_all` topic.

4. Run the `consumer.py` script to subscribe to `rides_all` and get a running top 5 of most popular location IDs:

```
$ python consumer.py
consuming from Kafka started
available topics to consume:  {'rides_all'}

---
|   pu_location_id |   count |
|-----------------:|--------:|
|              252 |       3 |
---
|   pu_location_id |   count |
|-----------------:|--------:|
|              252 |       3 |
|                7 |       3 |
|              256 |       3 |

...

---
|   pu_location_id |   count |
|-----------------:|--------:|
|              265 |      25 |
|               25 |       5 |
|              173 |       5 |
|              145 |       5 |
|               66 |       4
```
