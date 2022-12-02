#!/usr/bin/env python

import json
import logging
import math
from typing import Dict, List, Optional
from datetime import datetime, timezone

import googlemaps
import requests
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
# Quadkeys at minimum resolution based on inspecting requests from the outage ma.
QUADKEYS = [
    "02133",
    "02311",
    "02313",
    "03022",
    "03023",
    "03032",
    "03200",
    "03201",
    "03202",
    "03203",
    "03210",
    "03212",
]

SCHEMA = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("raw", "STRING"),
    bigquery.SchemaField("directory_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("scrape_timestamp", "TIMESTAMP"),
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
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField(
        "geom",
        "RECORD",
        fields=(
            bigquery.SchemaField("a", "STRING", "REPEATED"),
            bigquery.SchemaField("p", "STRING", "REPEATED"),
        ),
    ),
]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_directory(session: requests.Session) -> str:
    response = requests.get(f"{BASE_URL}{METADATA_URL}")
    response.raise_for_status()
    return response.json()["directory"]


def get_outages(
    session: requests.Session,
    directory: str,
    quadkey: str,
    directory_timestamp: datetime,
    scrape_timestamp: datetime,
) -> List[Dict]:
    path = OUTAGE_URL_TEMPLATE.format(directory=directory, quadkey=quadkey)
    url = f"{BASE_URL}{path}"
    logger.info(url)
    response = session.get(url)
    # Files for quadkeys with no outages don't exist and return a 403; ignore these errors but
    # crash on any others.
    if response.status_code == 403:
        return []
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
            "point_shape": polylines_to_linestring(row.get("geom", {}).get("p", [])),
            "area_shape": polylines_to_linestring(row.get("geom", {}).get("a", [])),
            # Save the raw data in case we need to parse it again later.
            "raw": json.dumps(row),
        }
    if len(quadkey) < MAX_QUADKEY_LENGTH:
        for quadrant in range(4):
            yield from get_outages(
                session,
                directory,
                f"{quadkey}{quadrant}",
                directory_timestamp=directory_timestamp,
                scrape_timestamp=scrape_timestamp,
            )


def marshal_timestamp(timestamp: datetime) -> str:
    # We expect timestamps to be in utc already
    assert timestamp.tzinfo == timezone.utc, f"got unexpected timezone {timestamp.tzinfo}"
    return timestamp.replace(tzinfo=None).isoformat()


def polylines_to_linestring(polylines: List[str]) -> Optional[str]:
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
        elif all(shape.type == "LineString" for shape in shapes):
            return str(sg.MultiLineString(shapes))
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
        return sg.LineString([[coord["lng"], coord["lat"]] for coord in decoded])


def main():
    session = requests.Session()
    bq_client = bigquery.Client()
    scrape_timestamp = datetime.now(timezone.utc)

    table = bigquery.Table(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", schema=SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="directory_timestamp",
    )

    directory = get_directory(session)
    directory_timestamp = datetime.strptime(directory, "%Y_%m_%d_%H_%M_%S").replace(tzinfo=timezone.utc)

    outages = []
    for quadkey in QUADKEYS:
        outages.extend(
            get_outages(
                session,
                directory,
                quadkey,
                directory_timestamp=directory_timestamp,
                scrape_timestamp=scrape_timestamp,
            )
        )

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


if __name__ == "__main__":
    main()
