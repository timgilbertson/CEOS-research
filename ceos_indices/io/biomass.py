import glob
import logging
import math
import os
import time
from typing import Iterator, List

import geopandas as gpd
from requests import Response
from requests_toolbelt.downloadutils import stream
from requests_toolbelt.exceptions import StreamingError
from requests_toolbelt.sessions import BaseUrlSession
from shapely.geometry import box

from inbound import load_sensor_locations

AUTH = (os.environ["VDS_USER"], os.environ["VDS_PASS"])
OUTPUT_FOLDER = "data/inputs/vandersat"


def create_subscription(session: BaseUrlSession, coordinates: List[List[float]]) -> dict:
    """Create a subscription and return it."""
    data = {
        "name": "Santa Rosa biomass",
        "api_name": "BIOMASS-PROXY_V2.0_10",
        "start_date": "2020-01-01",
        "end_date": "2022-01-01",
        "geojson": {
            "type": "Polygon",
            "coordinates": [coordinates],
        },
    }

    result = session.post(url="subscriptions", json=data).json()
    logging.info(f"Created subscription: {result}")
    return result


def get_all_pages(
    session: BaseUrlSession, url: str, page_size: int = 50
) -> Iterator[dict]:
    """Get a generator to fetch paginated API results page by page."""
    params = {"page": 1, "limit": page_size}
    first_page = session.get(url=url, params=params).json()
    yield first_page

    page_count = math.ceil(first_page["total_items"] / page_size)
    for params["page"] in range(2, page_count + 1):
        next_page = session.get(url=url, params=params).json()
        yield next_page


def get_subscriptions(session: BaseUrlSession):
    """Fetch the details of all subscriptions."""
    for page in get_all_pages(session, url="subscriptions"):
        for subscription in page["subscriptions"]:
            logging.info(f"Existing subscription: {subscription}")


def get_subscription(session: BaseUrlSession, subscription_uuid: str) -> dict:
    """Fetch the details of a single subscription."""
    subscription = session.get(url=f"subscriptions/{subscription_uuid}").json()
    logging.info(f"Fetched subscription: {subscription}")
    return subscription


def download_files(session: BaseUrlSession, urls: List[str], output_folder: str):
    """Save URL(s) using the Content-Disposition header's file name."""
    os.makedirs(output_folder, exist_ok=True)
    for url in urls:
        # Find existing files: assume the Content-Disposition header
        # uses the same name as the URL or at most adds a prefix, so a
        # wildcard search suffices. A real application should not rely
        # on that: use the fulfillment date or UUID to track handling.
        name = url.split("/")[-2]
        existing = glob.glob(os.path.join(output_folder, f"*{name}"))
        if existing:
            logging.info(f"Skipped existing file: name={existing[0]}; url={url}")
            continue

        r = session.get(url=url, stream=True)
        try:
            filename = stream.stream_response_to_file(r, path=output_folder)
            logging.info(f"Downloaded file: name={filename}")
        except StreamingError as e:
            logging.error(f"Failed to download file; error={str(e)}; url={url}")


def handle_fulfillment(
    session: BaseUrlSession, subscription_uuid: str, fulfillment: dict
) -> bool:
    """Handle a single fulfillment, like from an HTTP notification."""
    if fulfillment["status"] == "Ready":
        output_folder = os.path.join(OUTPUT_FOLDER, subscription_uuid)
        # Even if Ready, `files` may be empty, like if the requested
        # min_coverage was not met for non-field-based gridded-data
        download_files(session, urls=fulfillment["files"], output_folder=output_folder)

    # When handling an HTTP push notification only statuses Ready and
    # Error are expected, but the polling in this example may also
    # yield intermediate statuses such as Scheduled and Processing
    return fulfillment["status"] in ("Ready", "Error")


def get_subscription_fulfillments(
    session: BaseUrlSession, subscription_uuid: str
) -> bool:
    """Get fulfillments; not needed when using HTTP notifications."""
    url = f"subscriptions/{subscription_uuid}/fulfillments"
    pending = None
    for page in get_all_pages(session, url=url):
        logging.info(f"Fetched page of fulfillments: result={page}")
        for fulfillment in page["fulfillments"]:
            pending = pending or not handle_fulfillment(
                session, subscription_uuid=subscription_uuid, fulfillment=fulfillment
            )
    # True if fulfillment(s) found and all were handled, False otherwise
    return not pending


def response_hook(response: Response, *_args, **_kwargs):
    """Hook to get detailed error details from the response body."""
    if response.status_code >= 400:
        logging.error(
            f"Error invoking API: url={response.url}; code={response.status_code}; "
            f"reason={response.reason}; message={response.text}"
        )
        exit(response.status_code)


def _create_coordinates(sensors: gpd.GeoDataFrame) -> List[List[float]]:
    return list(sensors.to_crs("epsg:4326").geometry.unary_union.convex_hull.exterior.coords)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    session = BaseUrlSession(base_url="https://maps.vandersat.com/api/v2/")
    session.hooks["response"] = [response_hook]
    session.auth = AUTH
    # List the current subscriptions
    get_subscriptions(session)

    sensors = load_sensor_locations("ceos_indices/sr_sensor_plots.gpkg")
    coordinates = _create_coordinates(sensors)

    # Create a new subscription and get its UUID
    uuid = create_subscription(session, coordinates)["uuid"]

    # Fetch the subscription; not very useful in this example
    get_subscription(session, subscription_uuid=uuid)

    # NOTE: polling is not recommended, use `http_notify` instead
    while True:
        # Get the fulfillments and download the result files
        if get_subscription_fulfillments(session, subscription_uuid=uuid):
            break
        logging.info("Not done yet; sleeping 10 minutes")
        time.sleep(10 * 60)

    session.close()
