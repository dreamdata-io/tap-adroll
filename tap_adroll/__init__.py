#!/usr/bin/env python3
import sys
import singer

from singer import utils

from . import sync, discover, exceptions


REQUIRED_CONFIG_KEYS = ["access_token"]
LOGGER = singer.get_logger()


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        catalog = discover.discover()
        catalog.dump()
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover.discover()
        adroll_client = sync.AdRoll(
            config=args.config, state=args.state, catalog=catalog
        )
        try:
            adroll_client.sync()
        except exceptions.AdrollAPIQuotaExceeded:
            sys.exit(0)


if __name__ == "__main__":
    main()
