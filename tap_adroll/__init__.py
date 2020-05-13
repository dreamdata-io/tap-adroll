#!/usr/bin/env python3
import os
import json
import singer
import backoff
import requests
import ratelimit
from typing import Union
from datetime import timedelta, datetime
from dateutil import parser
from ratelimit import limits
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer import (
    utils,
    metadata,
    Transformer,
    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
)


STREAMS = {
    "advertisables": {"key_properties": "eid"},
    "campaigns": {"key_properties": "eid"},
    "deliveries": {"key_properties": "campaign_eid"},
}
REQUIRED_CONFIG_KEYS = ["start_date", "api_key", "access_token"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = json.load(file)
    return schemas


def discover():
    schemas = load_schemas()
    streams = []
    for tap_stream_id, props in STREAMS.items():
        key_properties = props.get("key_properties", [])
        schema = schemas.get(tap_stream_id)
        mdata = metadata.get_standard_metadata(
            schema=schema, key_properties=key_properties,
        )
        streams.append(
            CatalogEntry(
                stream=tap_stream_id,
                tap_stream_id=tap_stream_id,
                key_properties=key_properties,
                schema=Schema.from_dict(schema),
                metadata=mdata,
            )
        )
    return Catalog(streams)


class AdRoll:
    BASE_URL = "https://services.adroll.com/"

    def __init__(self, config, state, catalog, limit=250):
        self.SESSION = requests.Session()
        self.limit = limit
        self.access_token = config["access_token"]
        self.config = config
        self.state = state
        self.catalog = catalog
        self.accounts = None

    def sync(self):
        """ Sync data from tap source """
        state = self.state
        with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as transformer:
            for stream in self.catalog.get_selected_streams(state):
                LOGGER.info("Syncing stream:" + stream.tap_stream_id)

                singer.write_schema(
                    stream_name=stream.tap_stream_id,
                    schema=stream.to_dict(),
                    key_properties=stream.key_properties,
                )

                for row in self.get_streams(stream.tap_stream_id):
                    record = transformer.transform(
                        row, stream.schema.to_dict(), stream.metadata[0]
                    )

                    singer.write_records(stream.tap_stream_id, [record])

    def get_streams(self, tap_stream_id):
        if tap_stream_id == "advertisables":
            api_result = self.call_api(
                url="api/v1/organization/get_advertisables",
                params={"apikey": self.config["api_key"]},
            )
            self.accounts = api_result["results"]
            return self.accounts
        elif tap_stream_id == "campaigns":
            assert self.accounts and len(self.accounts) > 0

            for account in self.accounts:
                api_result = self.call_api(
                    url="api/v1/advertisable/get_campaigns_fast",  # 🏎️ 💨 💨
                    params={
                        "apikey": self.config["api_key"],
                        "advertisable": account["eid"],
                    },
                )
                self.campaigns = api_result["results"]
                return self.campaigns

    @backoff.on_exception(
        backoff.expo,
        (
            requests.exceptions.RequestException,
            requests.exceptions.ReadTimeout,
            requests.exceptions.HTTPError,
            ratelimit.exception.RateLimitException,
        ),
        max_tries=10,
    )
    @limits(calls=100, period=10)
    def call_api(self, url, params={}):
        url = f"{self.BASE_URL}{url}"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        response = self.SESSION.get(url, headers=headers, params=params)

        LOGGER.info(response.url)
        response.raise_for_status()
        return response.json()


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        catalog = discover()
        catalog.dump()
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        adroll_client = AdRoll(config=args.config, state=args.state, catalog=catalog)
        adroll_client.sync()


if __name__ == "__main__":
    main()
