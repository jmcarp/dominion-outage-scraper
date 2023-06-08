#!/usr/bin/env python

"""Scrape outage data from the Dominion Virginia map.

The Dominion outage map uses outage data stored as json and organized in time by directory
timestamps and in space by quadkeys. Directory timestamps are spaced roughly every 15 minutes, with
some jitter possibly related to processing or upload time. Quadkeys are a geospatial indexing
system that recursively divide space into quadrants, with each additional digit subdividing the
previous tile into four:

https://learn.microsoft.com/en-us/bingmaps/articles/bing-maps-tile-system

Fortunately, Dominion has multiple years of outage data available through this system, although
the format appears to be undocumented.
"""

import asyncio
import json
import logging
import math
import pathlib
from datetime import datetime, timedelta, timezone
from typing import AsyncIterable, Dict, List, Optional

import googlemaps
import httpx
import shapely.geometry as sg
from google.cloud import bigquery

BASE_URL = "https://outagemap.dominionenergy.com/resources/data/external/interval_generation_data/"
METADATA_URL = "metadata.json"
OUTAGE_URL_TEMPLATE = "{directory}/outages/{quadkey}.json"

PROJECT_ID = "cvilledata"
DATASET_ID = "dominion"
TABLE_ID = "outages"

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

SCHEMA = [
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


async def get_current_directory(client: httpx.AsyncClient) -> str:
    """Get the currect "directory" timestamp."""
    response = await client.get(f"{BASE_URL}{METADATA_URL}")
    response.raise_for_status()
    return response.json()["directory"]


async def guess_directory(
    client: httpx.AsyncClient,
    timestamp: datetime,
    max_offset: timedelta = timedelta(minutes=15),
) -> Optional[str]:
    """Guess directory for a given timestamp.

    The Dominion outage map uses a "directory" to represent the timestamp at which a batch of files
    was made available via their undocumented API. Directory timestamps appear to increment in 30s
    intervals and usually occur within a few minutes of the 15 minute mark. The API includes an
    endpoint to look up the most recent directory, but to check directories for older time points,
    we have to guess: start guessing at the timestamp of interest, then increment by 30s intervals
    until we either find a valid directory or reach the maximum offset.
    """
    for quadkey in QUADKEYS:
        offset = timedelta()
        while offset < max_offset:
            directory = (timestamp + offset).strftime("%Y_%m_%d_%H_%M_%S")
            path = OUTAGE_URL_TEMPLATE.format(directory=directory, quadkey=quadkey)
            url = f"{BASE_URL}{path}"
            logger.debug(url)
            response = await client.get(url)
            if response.status_code == 200:
                logger.info(f"guessed directory {directory} from quadkey {quadkey}")
                return directory
            offset += timedelta(seconds=30)
    return None


async def get_outages(
    client: httpx.AsyncClient,
    directory: str,
    quadkey: str,
    directory_timestamp: datetime,
    scrape_timestamp: datetime,
) -> AsyncIterable[dict]:
    """Recursively search a quadkey at a directory timestamp for outages.

    Look up outages for the specified quadkey. Then, check each of its child quadkeys (0, 1, 2, 3)
    for more detailed outage information, recursing until we reach an empty key or the maximum
    quadkey length.
    """
    path = OUTAGE_URL_TEMPLATE.format(directory=directory, quadkey=quadkey)
    url = f"{BASE_URL}{path}"
    logger.info(url)
    response = await client.get(url)
    # Files for quadkeys with no outages don't exist and return a 403; ignore these errors but
    # crash on any others.
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

    # Search children of quadkey to get more fine-grained details about outages. Note that we could
    # potentially search more efficiently by intersecting potential child quadkeys with the
    # geometries of the outages, but this is more complicated and possibly more error-prone. Prefer
    # simpler brute-force approach.
    if len(quadkey) < MAX_QUADKEY_LENGTH:
        for quadrant in range(4):
            async for outage in get_outages(
                client,
                directory,
                f"{quadkey}{quadrant}",
                directory_timestamp=directory_timestamp,
                scrape_timestamp=scrape_timestamp,
            ):
                yield outage


def marshal_timestamp(timestamp: datetime) -> str:
    """Marshal a timestamp as an iso-formatted string so that we can serialize to json."""
    # We expect timestamps to be in utc already.
    assert (
        timestamp.tzinfo == timezone.utc
    ), f"got unexpected timezone {timestamp.tzinfo}"
    return timestamp.replace(tzinfo=None).isoformat()


def polylines_to_shape(polylines: List[str]) -> Optional[str]:
    """Convert a list of encoded polylines to a stringified shape.

    Note: Dominion seems to use a single point for geom["p"] and a single line for geom["a"], but
    both structures are arrays, so we try to handle multi-point and multi-line geometries. We don't
    want to break ingestion if we fail to parse polyline, so warn on errors and return None.
    """
    if len(polylines) == 0:
        return None
    shapes = [polyline_to_shape(polyline) for polyline in polylines]
    # Ignore errors in polyline parsing
    shapes = [shape for shape in shapes if shape is not None]
    if len(shapes) == 1:
        return str(shapes[0])
    else:
        if all(shape.type == "Point" for shape in shapes):
            return str(sg.MultiPoint(shapes))
        elif all(shape.type == "Polygon" for shape in shapes):
            return str(sg.MultiPolygon(shapes))
        else:
            shape_types = [shape.type for shape in shapes]
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
    timestamp: datetime,
    scrape_timestamp: datetime,
    retry_failed=False,
) -> None:
    """Scrape outages for a given timestamp and write them to disk."""
    path, tmp_path = path_for_timestamp(timestamp), path_for_timestamp(
        timestamp, ".json.tmp"
    )
    # For some timestamps we aren't able to guess a directory, and guessing is expensive when this
    # happens because we check every possible directory. To avoid repeat guessing, touch the output
    # path when we start scraping, and skip scrapes if the path already exists. We can override this
    # behavior by passing `retry_failed=True`.
    if path.exists():
        should_retry = retry_failed and path.stat().st_size == 0
        if not (retry_failed and path.stat().st_size == 0):
            logger.info(f"path {path} exists; skipping timestamp")
            return
    directory = await guess_directory(client, timestamp)
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
            directory,
            quadkey,
            directory_timestamp=directory_timestamp,
            scrape_timestamp=scrape_timestamp,
        ):
            timestamp_outages.append(outage)

    # Atomically write outages to disk by writing to temp path and then replacing final path.
    with tmp_path.open("w") as fp:
        for outage in timestamp_outages:
            fp.write(json.dumps(outage))
            fp.write("\n")
    tmp_path.replace(path)


async def scrape_interval(
    client: httpx.AsyncClient,
    timestamps: List[datetime],
    max_concurrency=1,
    retry_failed=False,
):
    scraped_at = datetime.now(timezone.utc)
    tasks = [
        scrape_timestamp(client, timestamp, scraped_at, retry_failed=retry_failed)
        for timestamp in timestamps
    ]
    results = await gather_with_semaphore(
        max_concurrency, *tasks, return_exceptions=True
    )
    errors = [result for result in results if isinstance(result, Exception)]
    if len(errors) > 0:
        raise RuntimeError(errors)


def path_for_timestamp(timestamp: datetime, suffix=".json") -> pathlib.Path:
    return pathlib.Path("./outages").joinpath(
        f"{timestamp.strftime('%Y_%m_%d_%H_%M')}{suffix}"
    )


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


def load_outages(bq_client: bigquery.Client, timestamps: List[datetime]) -> None:
    table = bigquery.Table(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", schema=SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="directory_timestamp",
    )

    def outages():
        for timestamp in timestamps:
            path = path_for_timestamp(timestamp)
            with path.open() as fp:
                for line in fp.readlines():
                    yield json.loads(line)

    bq_client.load_table_from_json(
        outages(),
        table,
        # Try to accommodate changes in data shape
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            schema_update_options=["ALLOW_FIELD_ADDITION"],
            ignore_unknown_values=True,
            allow_jagged_rows=True,
            schema=table.schema,
        ),
    ).result()


def main():
    client = httpx.Client(verify=False)
    bq_client = bigquery.Client()
    scrape_timestamp = datetime.now(timezone.utc)

    table = bigquery.Table(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", schema=SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="directory_timestamp",
    )

    directory = get_current_directory(client)
    directory_timestamp = datetime.strptime(directory, "%Y_%m_%d_%H_%M_%S").replace(
        tzinfo=timezone.utc
    )

    outages = []
    for quadkey in QUADKEYS:
        outages.extend(
            get_outages(
                client,
                directory,
                quadkey,
                directory_timestamp=directory_timestamp,
                scrape_timestamp=scrape_timestamp,
            )
        )

    logger.info(f"collected {len(outages)} outages")

    bq_client.load_table_from_json(
        outages,
        table,
        # Try to accommodate changes in data shape
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            schema_update_options=["ALLOW_FIELD_ADDITION"],
            ignore_unknown_values=True,
            allow_jagged_rows=True,
            schema=table.schema,
        ),
    ).result()

    logger.info("wrote outages to bigquery")


def entrypoint(event, context):
    """Google cloud functions entrypoint."""
    main()


if __name__ == "__main__":
    main()
