#!/usr/bin/env python3
import sys
import singer

from singer import utils

from . import sync


REQUIRED_CONFIG_KEYS = ["access_token"]
LOGGER = singer.get_logger()


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    adroll_client = sync.AdRoll(config=args.config, state=args.state)
    adroll_client.sync(streams=["advertisables", "campaigns", "deliveries"])


if __name__ == "__main__":
    main()
