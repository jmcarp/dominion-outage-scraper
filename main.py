#!/usr/bin/env python

"""Scrape outage data from the Dominion Virginia map.

The Dominion outage map uses outage data stored as json and organized in time
by directory timestamps and in space by quadkeys. Directory timestamps are
spaced roughly every 15 minutes, with some jitter possibly related to
processing or upload time. Quadkeys are a geospatial indexing system that
recursively divide space into quadrants, with each additional digit subdividing
the previous tile into four:

https://learn.microsoft.com/en-us/bingmaps/articles/bing-maps-tile-system

Fortunately, Dominion has multiple years of outage data available through this
system, although the format appears to be undocumented.

We scrape these records to disk, using one json file per directory timestamp,
so that records are human-readable and easy to retry by timestamp. Before
uploading to cloud storage for archival, we convert to monthly parquet files
for better compression and performance.
"""

import argparse
import asyncio
import json
import logging
import math
import pathlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import AsyncIterable, Dict, List, Optional

import duckdb
import googlemaps
import httpx
import shapely.geometry as sg
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery


@dataclass
class Config:
    base_url: str
    base_path: pathlib.Path
    parquet_path: pathlib.Path
    label: str

    metadata_url: str = "metadata.json"
    outage_url_template: str = "{directory}/outages/{quadkey}.json"

    def get_metadata_url(self) -> str:
        return f"{self.base_url}{self.metadata_url}"

    def get_outage_url(self, directory: str, quadkey: str) -> str:
        path = self.outage_url_template.format(directory=directory, quadkey=quadkey)
        return f"{self.base_url}{path}"

    def path_for_timestamp(self, timestamp: datetime, suffix=".json") -> pathlib.Path:
        return self.base_path.joinpath(
            f"{timestamp.strftime('%Y_%m_%d_%H_%M')}{suffix}"
        )


DominionConfig = Config(
    base_url="https://outagemap.dominionenergy.com/resources/data/external/interval_generation_data/",
    base_path=pathlib.Path("./outages/dominion"),
    parquet_path=pathlib.Path("./parquet/dominion"),
    label="dominion",
)

AppalachianConfig = Config(
    base_url="https://d2oclp3li76tyy.cloudfront.net/resources/data/external/interval_generation_data/",
    base_path=pathlib.Path("./outages/appalachian"),
    parquet_path=pathlib.Path("./parquet/appalachian"),
    label="appalachian",
)

CONFIGS = {
    "dominion": DominionConfig,
    "appalachian": AppalachianConfig,
}


PROJECT_ID = "cvilledata"
DATASET_ID = "dominion"
TABLE_ID = "outages"
BUCKET_NAME = "dominion-outage-data"

# Max quadkey length based on inspecting requests from the outage map.
MAX_QUADKEY_LENGTH = 14
# Quadkeys at minimum resolution based on inspecting requests from the outage map.
# TODO: Determine which quadkeys are part of the Dominion service area to save time on unnecessary
# requests.
QUADKEYS = [
    "03200",
    "03201",
    "02133",
    "02311",
    "02313",
    "03022",
    "03023",
    "03032",
    "03202",
    "03203",
    "03210",
    "03212",
]

# TODO: Generate parquet colums from bigquery schema, or vice versa.
PARQUET_COLUMNS = {
    "id": "VARCHAR",
    "title": "VARCHAR",
    "file_title": "VARCHAR",
    "desc": """STRUCT(
        crew_icon BOOL,
        cause VARCHAR,
        crew_status VARCHAR,
        start_etr VARCHAR,
        end_etr VARCHAR,
        cust_a STRUCT(
            val INT
        ),
        cluster BOOL,
        inc_id VARCHAR,
        n_out INT
    )""",
    "geom": "STRUCT(p VARCHAR[], a VARCHAR[])",
    "directory_timestamp": "TIMESTAMP",
    "scrape_timestamp": "TIMESTAMP",
    "point_shape": "VARCHAR",
    "area_shape": "VARCHAR",
    "raw": "VARCHAR",
}

BIGQUERY_SCHEMA = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("file_title", "STRING"),
    bigquery.SchemaField(
        "desc",
        "RECORD",
        fields=(
            bigquery.SchemaField("crew_icon", "BOOLEAN"),
            bigquery.SchemaField("cause", "STRING"),
            bigquery.SchemaField("crew_status", "STRING"),
            # Treat nested timestamps as strings to avoid parse errors
            bigquery.SchemaField("start_etr", "STRING"),
            bigquery.SchemaField("end_etr", "STRING"),
            bigquery.SchemaField(
                "cust_a",
                "RECORD",
                fields=(bigquery.SchemaField("val", "INTEGER"),),
            ),
            bigquery.SchemaField("cluster", "BOOLEAN"),
            bigquery.SchemaField("inc_id", "STRING"),
            bigquery.SchemaField("n_out", "INTEGER"),
        ),
    ),
    bigquery.SchemaField(
        "geom",
        "RECORD",
        fields=(
            bigquery.SchemaField("a", "STRING", "REPEATED"),
            bigquery.SchemaField("p", "STRING", "REPEATED"),
        ),
    ),
    bigquery.SchemaField("directory_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("scrape_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("point_shape", "STRING"),
    bigquery.SchemaField("area_shape", "STRING"),
    bigquery.SchemaField("raw", "STRING"),
]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def get_current_directory(client: httpx.AsyncClient, config: Config) -> str:
    """Get the currect "directory" timestamp."""
    response = await client.get(config.get_metadata_url())
    response.raise_for_status()
    return response.json()["directory"]


async def guess_directory(
    client: httpx.AsyncClient,
    config: Config,
    timestamp: datetime,
    max_offset: timedelta = timedelta(minutes=15),
) -> Optional[str]:
    """Guess directory for a given timestamp.

    The Dominion outage map uses a "directory" to represent the timestamp at
    which a batch of files was made available via their undocumented API.
    Directory timestamps appear to increment in 30s intervals and usually occur
    within a few minutes of the 15 minute mark. The API includes an endpoint to
    look up the most recent directory, but to check directories for older time
    points, we have to guess: start guessing at the timestamp of interest, then
    increment by 30s intervals until we either find a valid directory or reach
    the maximum offset.
    """
    for quadkey in QUADKEYS:
        offset = timedelta()
        while offset < max_offset:
            directory = (timestamp + offset).strftime("%Y_%m_%d_%H_%M_%S")
            url = config.get_outage_url(directory, quadkey)
            logger.debug(url)
            response = await client.get(url)
            if response.status_code == 200:
                logger.info(f"guessed directory {directory} from quadkey {quadkey}")
                return directory
            offset += timedelta(seconds=30)
    return None


async def get_outages(
    client: httpx.AsyncClient,
    config: Config,
    directory: str,
    quadkey: str,
    directory_timestamp: datetime,
    scrape_timestamp: datetime,
) -> AsyncIterable[dict]:
    """Recursively search a quadkey at a directory timestamp for outages.

    Look up outages for the specified quadkey. Then, check each of its child
    quadkeys (0, 1, 2, 3) for more detailed outage information, recursing until
    we reach an empty key or the maximum quadkey length.
    """
    url = config.get_outage_url(directory, quadkey)
    logger.info(url)
    response = await client.get(url)
    # Files for quadkeys with no outages don't exist and return a 403; ignore
    # these errors but crash on any others.
    if response.status_code == 403:
        return
    response.raise_for_status()
    data = response.json()
    for row in data["file_data"]:
        yield {
            **row,
            "file_title": data["file_title"],
            # In theory this is always the same as the file title.
            "directory": directory,
            "quadkey": quadkey,
            "scrape_timestamp": marshal_timestamp(scrape_timestamp),
            "directory_timestamp": marshal_timestamp(directory_timestamp),
            "point_shape": polylines_to_shape(row.get("geom", {}).get("p", [])),
            "area_shape": polylines_to_shape(row.get("geom", {}).get("a", [])),
            # Save the raw data in case we need to parse it again later.
            "raw": json.dumps(row),
        }

    # Search children of quadkey to get more fine-grained details about
    # outages. Note that we could potentially search more efficiently by
    # intersecting potential child quadkeys with the geometries of the outages,
    # but this is more complicated and possibly more error-prone. Prefer
    # simpler brute-force approach.
    if len(quadkey) < MAX_QUADKEY_LENGTH:
        for quadrant in range(4):
            async for outage in get_outages(
                client,
                config,
                directory,
                f"{quadkey}{quadrant}",
                directory_timestamp=directory_timestamp,
                scrape_timestamp=scrape_timestamp,
            ):
                yield outage


def marshal_timestamp(timestamp: datetime) -> str:
    """Marshal a timestamp as an iso-formatted string so that we can serialize
    to json."""
    # We expect timestamps to be in utc already.
    assert (
        timestamp.tzinfo == timezone.utc
    ), f"got unexpected timezone {timestamp.tzinfo}"
    return timestamp.replace(tzinfo=None).isoformat()


def polylines_to_shape(polylines: List[str]) -> Optional[str]:
    """Convert a list of encoded polylines to a stringified shape.

    Note: Dominion seems to use a single point for geom["p"] and a single line
    for geom["a"], but both structures are arrays, so we try to handle
    multi-point and multi-line geometries. We don't want to break ingestion if
    we fail to parse polyline, so warn on errors and return None.
    """
    if len(polylines) == 0:
        return None
    shapes = [polyline_to_shape(polyline) for polyline in polylines]
    # Ignore errors in polyline parsing
    valid_shapes = [shape for shape in shapes if shape is not None]
    if len(shapes) == 1:
        return str(shapes[0])
    else:
        if all(shape.type == "Point" for shape in valid_shapes):
            return str(sg.MultiPoint(shapes))
        elif all(shape.type == "Polygon" for shape in valid_shapes):
            return str(sg.MultiPolygon(shapes))
        else:
            shape_types = [shape.type for shape in valid_shapes]
            logger.warning(f"got mismatched types: {', '.join(shape_types)}")
            return None


def polyline_to_shape(polyline: str) -> Optional[sg.base.BaseGeometry]:
    try:
        decoded = googlemaps.convert.decode_polyline(polyline)
    except Exception:
        logger.exception(f"failed to parse encoded polyline {polyline}")
        return None
    if len(decoded) == 0:
        logger.warning(f"got empty polyline from encoded polyline {polyline}")
        return None
    if len(decoded) == 1:
        return sg.Point(decoded[0]["lng"], decoded[0]["lat"])
    else:
        return sg.Polygon([[coord["lng"], coord["lat"]] for coord in decoded])


def date_range(start, stop, interval):
    while start < stop:
        yield start
        start += interval


async def scrape_timestamp(
    client: httpx.AsyncClient,
    config: Config,
    timestamp: datetime,
    scrape_timestamp: datetime,
    retry_failed=False,
) -> None:
    """Scrape outages for a given timestamp and write them to disk."""
    path = config.path_for_timestamp(timestamp)
    tmp_path = config.path_for_timestamp(timestamp, ".json.tmp")

    # For some timestamps we aren't able to guess a directory, and guessing is
    # expensive when this happens because we check every possible directory. To
    # avoid repeat guessing, touch the output path when we start scraping, and
    # skip scrapes if the path already exists. We can override this behavior by
    # passing `retry_failed=True`.
    if path.exists():
        should_retry = retry_failed and path.stat().st_size == 0
        if not (retry_failed and path.stat().st_size == 0):
            logger.info(f"path {path} exists; skipping timestamp")
            return
    directory = await guess_directory(client, config, timestamp)
    tmp_path.touch()
    if directory is None:
        logger.warning(
            f"couldn't guess directory for timestamp {timestamp.isoformat()}"
        )
        return
    directory_timestamp = datetime.strptime(directory, "%Y_%m_%d_%H_%M_%S").replace(
        tzinfo=timezone.utc
    )
    timestamp_outages = []
    for quadkey in QUADKEYS:
        async for outage in get_outages(
            client,
            config,
            directory,
            quadkey,
            directory_timestamp=directory_timestamp,
            scrape_timestamp=scrape_timestamp,
        ):
            timestamp_outages.append(outage)

    # Atomically write outages to disk by writing to temp path and then
    # replacing final path.
    with tmp_path.open("w") as fp:
        for outage in timestamp_outages:
            fp.write(json.dumps(outage))
            fp.write("\n")
    tmp_path.replace(path)


async def scrape_interval(
    client: httpx.AsyncClient,
    config: Config,
    timestamps: List[datetime],
    max_concurrency=1,
    retry_failed=False,
):
    scraped_at = datetime.now(timezone.utc)
    tasks = [
        scrape_timestamp(
            client, config, timestamp, scraped_at, retry_failed=retry_failed
        )
        for timestamp in timestamps
    ]
    results = await gather_with_semaphore(
        max_concurrency, *tasks, return_exceptions=True
    )
    errors = [result for result in results if isinstance(result, Exception)]
    if len(errors) > 0:
        raise RuntimeError(errors)


async def gather_with_semaphore(n, *tasks, **kwargs):
    """Gather tasks using a semaphore to limit concurrency.

    Taken from https://stackoverflow.com/a/61478547."""
    semaphore = asyncio.Semaphore(n)

    async def task_with_semaphore(task):
        async with semaphore:
            return await task

    return await asyncio.gather(
        *(task_with_semaphore(task) for task in tasks), **kwargs
    )


def load_outages(bq_client: bigquery.Client, config: Config) -> None:
    table = bigquery.Table(
        f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}_{config.label}", schema=BIGQUERY_SCHEMA
    )
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="directory_timestamp",
    )

    bq_client.load_table_from_uri(
        [f"gs://{BUCKET_NAME}/parquet/{config.label}/*.parquet"],
        table,
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            source_format="PARQUET",
            schema=table.schema,
        ),
    ).result()


async def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True)

    fetch_parser = subparsers.add_parser("fetch")
    fetch_parser.add_argument("config", choices=CONFIGS.keys())
    fetch_parser.add_argument("start_date", type=datetime.fromisoformat)
    fetch_parser.add_argument("end_date", type=datetime.fromisoformat)
    fetch_parser.add_argument(
        "--interval-minutes",
        type=lambda value: timedelta(minutes=int(value)),
        default=timedelta(minutes=15),
    )
    fetch_parser.add_argument("--max-concurrency", type=int, default=20)
    fetch_parser.add_argument("--retry-failed", action="store_true")
    fetch_parser.set_defaults(func=fetch)

    compact_parser = subparsers.add_parser("compact")
    compact_parser.add_argument("config", choices=CONFIGS.keys())
    compact_parser.add_argument("start_date", type=datetime.fromisoformat)
    compact_parser.add_argument("end_date", type=datetime.fromisoformat)
    compact_parser.set_defaults(func=compact)

    load_parser = subparsers.add_parser("load")
    load_parser.add_argument("config", choices=CONFIGS.keys())
    load_parser.set_defaults(func=load)

    args = parser.parse_args()
    await args.func(args)


async def fetch(args):
    config = CONFIGS[args.config]
    client = httpx.AsyncClient(verify=False, timeout=30)
    await scrape_interval(
        client,
        config,
        date_range(args.start_date, args.end_date, args.interval_minutes),
        max_concurrency=args.max_concurrency,
        retry_failed=args.retry_failed,
    )


async def compact(args):
    config = CONFIGS[args.config]
    start_month = datetime(args.start_date.year, args.start_date.month, 1)
    end_month = datetime(args.end_date.year, args.end_date.month, 1)
    month = start_month
    while month < end_month:
        duckdb.read_json(
            str(config.base_path.joinpath(f"{month.year}_{month.month:02d}_*")),
            format="auto",
            columns=PARQUET_COLUMNS,
        ).write_parquet(
            str(
                config.parquet_path.joinpath(f"{month.year}_{month.month:02d}.parquet")
            ),
        ),
        month += relativedelta(months=1)


async def load(args):
    bq_client = bigquery.Client()
    config = CONFIGS[args.config]
    load_outages(bq_client, config)


if __name__ == "__main__":
    asyncio.run(main())
