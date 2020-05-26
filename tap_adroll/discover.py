import os
import json

from singer import metadata
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry


STREAMS = {
    "advertisables": {"key_properties": "eid", "replication_method": "FULL_TABLE"},
    "campaigns": {"key_properties": "eid", "replication_method": "FULL_TABLE"},
    "deliveries": {
        "key_properties": "campaign_eid",
        "replication_method": "INCREMENTAL",
        "valid_replication_keys": ["date"],
    },
}


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
        valid_replication_keys = props.get("valid_replication_keys", [])
        replication_method = props.get("replication_method")

        schema = schemas.get(tap_stream_id)
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=key_properties,
            valid_replication_keys=valid_replication_keys,
            replication_method=replication_method,
        )
        streams.append(
            CatalogEntry(
                stream=tap_stream_id,
                tap_stream_id=tap_stream_id,
                key_properties=key_properties,
                schema=Schema.from_dict(schema),
                metadata=mdata,
                replication_method=replication_method,
            )
        )
    return Catalog(streams)
