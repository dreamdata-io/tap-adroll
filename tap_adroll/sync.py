import backoff
import json
import requests
import singer
import sys
from typing import Union, List, Tuple
from datetime import datetime, date, timedelta
from dateutil import parser
from ratelimit import limits, exception
from singer import (
    Transformer,
    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
)


LOGGER = singer.get_logger()


def date_chunks(
    start_date: date, increment: timedelta, maximum: date = None
) -> List[Tuple[date, date]]:
    start = start_date
    if not maximum:
        maximum = datetime.utcnow().date() - timedelta(days=1)

    while True:
        end_date = min(start + increment, maximum)
        yield (start, end_date)
        start = end_date
        if end_date == maximum:
            return


class AdRoll:
    BASE_URL = "https://services.adroll.com/"

    def __init__(self, config, state, catalog, limit=250):
        self.SESSION = requests.Session()
        self.limit = limit
        self.access_token = config["access_token"]
        self.config = config
        self.state = state
        self.catalog = catalog
        self.advertisables = None
        self.transformer = Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING)
        self.active_campaigns = []

    def sync(self):
        """ Sync data from tap source """
        for stream in self.catalog.get_selected_streams(self.state):
            LOGGER.info("Syncing stream:" + stream.tap_stream_id)
            singer.write_schema(
                stream_name=stream.tap_stream_id,
                schema=stream.schema.to_dict(),
                key_properties=stream.key_properties,
            )

            if stream.tap_stream_id == "deliveries":
                self.sync_deliveries(stream)
            else:
                self.sync_full_table_streams(stream)

    def sync_full_table_streams(self, stream):
        for row in self.get_streams(stream.tap_stream_id):
            record = self.transformer.transform(
                row, stream.schema.to_dict(), stream.metadata[0]
            )
            singer.write_records(stream.tap_stream_id, [record])

    def get_streams(self, tap_stream_id):
        if tap_stream_id == "advertisables":
            return self.get_advertisables()
        elif tap_stream_id == "campaigns":
            return self.get_campaigns()
        else:
            LOGGER.info(f"UNKNOWN STREAM: {tap_stream_id}")
            return []

    def get_advertisables(self):
        self.advertisables = self.call_api(url="api/v1/organization/get_advertisables",)
        return json.loads(
            json.dumps(self.advertisables), parse_int=str, parse_float=str
        )

    def get_campaigns(self):
        campaigns = []
        if self.advertisables and len(self.advertisables) > 0:
            for advertisable in self.advertisables:
                campaigns += self.call_api(
                    url="api/v1/advertisable/get_campaigns_fast",  # 🏎️ 💨 💨
                    params={"advertisable": advertisable["eid"]},
                )

        campaigns = [
            {
                "eid": campaign["eid"],
                "advertisable": campaign["advertisable"],
                "start_date": campaign["start_date"],
                "created_date": campaign["created_date"],
                "end_date": campaign["end_date"],
                "is_active": campaign["is_active"],
                "updated_date": campaign["updated_date"],
            }
            for campaign in campaigns
        ]

        self.active_campaigns = campaigns
        return json.loads(json.dumps(campaigns), parse_int=str, parse_float=str)

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, exception.RateLimitException),
        max_tries=5,
        factor=2,
        giveup=lambda e: e.response.status_code in [429],  # too many requests
    )
    @limits(calls=100, period=10)
    def call_api(self, url, params={}):
        url = f"{self.BASE_URL}{url}"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        response = self.SESSION.get(url, headers=headers, params=params)

        LOGGER.info(response.url)
        response.raise_for_status()
        response_json = response.json()

        return response_json["results"]

    def sync_deliveries(self, stream):
        state = self.state
        for campaign in self.active_campaigns:
            eid = campaign.get("eid")
            if not eid:
                LOGGER.error(f"{campaign} has no attribute 'eid'")
                continue
            # date of last sync, otherwise campaign start
            start_date = self.get_campaign_sync_start_date(stream, state, campaign)
            # date of campaign end if ended, otherwise None
            campaign_end_date = self.get_campaign_end_date(campaign)

            # everything synced and campaign ended (not active)
            if (start_date >= campaign_end_date) and not campaign["is_active"]:
                LOGGER.info(
                    f"(skipping) campaign: {eid} start_date: {start_date} end_date: {campaign_end_date}"
                )
                continue

            LOGGER.info(
                f"(syncing) campaign: {eid} start_date: {start_date} end_date: {campaign_end_date}"
            )
            state = self.bulk_read_campaign_deliveries_from_dates(
                stream=stream,
                state=state,
                campaign=campaign,
                start_date=start_date,
                campaign_end_date=campaign_end_date,
            )

    def bulk_read_campaign_deliveries_from_dates(
        self, stream, state, campaign, start_date, campaign_end_date
    ):
        end_date = min(start_date + timedelta(weeks=12), campaign_end_date)
        while end_date <= campaign_end_date:
            eid = campaign["eid"]
            LOGGER.info(
                f"(advancing) campaign: {eid} start_date: {start_date} end_date: {end_date}"
            )
            api_result = self.get_campaign_deliveries(campaign, start_date, end_date)
            if not api_result:
                continue
            state = self.write_campaign_deliveries_records_and_advance_state(
                stream, state, campaign, api_result
            )
            if end_date == campaign_end_date:
                break
            else:
                start_date = end_date
                end_date = min(end_date + timedelta(weeks=12), campaign_end_date)

        return state

    def get_campaign_sync_start_date(self, stream, state, campaign):
        if state and state.get("bookmarks", {}).get(stream.tap_stream_id, None):
            synced_campaigns = state["bookmarks"][stream.tap_stream_id]
            if synced_campaigns and synced_campaigns.get(campaign["eid"], None):
                return datetime.strptime(
                    synced_campaigns[campaign["eid"]], "%Y-%m-%dT%H:%M:%S"
                ).date()

        campaign_start_date = campaign.get("start_date") or campaign.get("created_date")
        campaign_start_date = datetime.strptime(
            campaign_start_date, "%Y-%m-%dT%H:%M:%S%z"
        ).replace(tzinfo=None)
        return campaign_start_date.date()

    def get_campaign_end_date(self, campaign):
        campaign_end_date = campaign.get("end_date")
        if campaign_end_date:
            return (
                datetime.strptime(campaign_end_date, "%Y-%m-%dT%H:%M:%S%z")
                .replace(tzinfo=None)
                .date()
            )

    def write_campaign_deliveries_records_and_advance_state(
        self, stream, state, campaign, api_result
    ):
        for summary in api_result["date"]:
            row = {
                "campaign_eid": campaign["eid"],
                "advertisable_eid": campaign["advertisable"],
                **summary,
            }
            record = self.transformer.transform(
                row, stream.schema.to_dict(), stream.metadata[0],
            )
            singer.write_records(stream.tap_stream_id, [record])

        # write the state here for the entire week batch (last record from summary)
        bookmark_key = campaign["eid"]
        prev_bookmark = api_result["date"][-1]["date"]
        return self.__advance_bookmark(
            state,
            bookmark=prev_bookmark,
            tap_stream_id=stream.tap_stream_id,
            bookmark_key=bookmark_key,
        )

    def get_campaign_deliveries(self, campaign, start_date, end_date):
        try:
            return self.call_api(
                url="uhura/v1/deliveries/campaign",
                params={
                    "breakdowns": "date",
                    "currency": "USD",
                    "advertisable_eid": campaign["advertisable"],
                    "campaign_eids": campaign["eid"],
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date.strftime("%Y-%m-%d"),
                },
            )
        except requests.exceptions.HTTPError as exc:
            if exc.response.status_code in [429]:
                LOGGER.info(exc)
                sys.exit()
            raise

    def __advance_bookmark(self, state, bookmark, tap_stream_id, bookmark_key):
        if not bookmark:
            singer.write_state(state)
            return state

        if isinstance(bookmark, datetime):
            bookmark_datetime = bookmark
        elif isinstance(bookmark, str):
            bookmark_datetime = parser.isoparse(bookmark)
        else:
            raise ValueError(
                f"bookmark is of type {type(bookmark)} but must be either string or datetime"
            )

        state = singer.write_bookmark(
            state, tap_stream_id, bookmark_key, bookmark_datetime.isoformat()
        )
        singer.write_state(state)
        return state
