#!/usr/bin/env python3
import os
import json
import singer
from typing import Union
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer import (
    utils,
    metadata,
)
from . import sync


STREAMS = {
    "advertisables": {"key_properties": "eid"},
    "campaigns": {"key_properties": "eid"},
    "deliveries": {"key_properties": "campaign_eid"},
}
REQUIRED_CONFIG_KEYS = ["access_token"]
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
        adroll_client = sync.AdRoll(
            config=args.config, state=args.state, catalog=catalog
        )
        adroll_client.sync()


if __name__ == "__main__":
    main()
