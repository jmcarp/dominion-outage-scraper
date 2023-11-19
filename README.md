# dominion-outage-scraper

## Usage

Collect data:

```
./main.py fetch dominion 2019-01-01 2020-01-01
./main.py fetch appalachian 2019-01-01 2020-01-01 --retry-failed
```

Convert to parquet:

```
./main.py compact dominion 2019-01-01 2020-01-01
```

Upload to gcs:

```
gsutil -m rsync -r outages gs://dominion-outage-data/parquet/
```

Load to bigquery:

```
./main.py load dominion
./main.py load appalachian
```
