#!/usr/bin/env python3
import os
import json
import singer
import backoff
import datetime
import requests
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema


STREAMS = {
    "advertisables": {
        "valid_replication_keys": ["updated_date"],
        "key_properties": "eid",
    },
    "campaigns": {"valid_replication_keys": ["updated_date"], "key_properties": "eid",},
}
REQUIRED_CONFIG_KEYS = ["start_date", "api_key", "refresh_token"]
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
            schema=schema,
            key_properties=key_properties,
            valid_replication_keys=props.get("valid_replication_keys", []),
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


def get_campaigns():
    import ipdb

    ipdb.set_trace()
    # for x in range(1000):
    #     yield x


def streams(tap_stream_id):
    if tap_stream_id == "campaigns":
        yield get_campaigns()


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = stream.replication_key
        is_sorted = (
            True  # TODO: indicate whether data is sorted ascending on bookmark value
        )

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.to_dict(),
            key_properties=stream.key_properties,
        )

        streams(stream.tap_stream_id)

        # TODO: delete and replace this inline function with your own data retrieval process:
        tap_data = lambda: [{"id": x, "name": "row${x}"} for x in range(1000)]

        max_bookmark = None
        for row in tap_data():
            # TODO: place type conversions or transformations here

            # write one or more rows to the stream:
            singer.write_records(stream.tap_stream_id, [row])
            if bookmark_column:
                if is_sorted:
                    # update bookmark to latest value
                    singer.write_state({stream.tap_stream_id: row[bookmark_column]})
                else:
                    # if data unsorted, save max value until end of writes
                    max_bookmark = max(max_bookmark, row[bookmark_column])
        if bookmark_column and not is_sorted:
            singer.write_state({stream.tap_stream_id: max_bookmark})
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
