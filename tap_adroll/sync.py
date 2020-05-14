import backoff
import json
import requests
import singer
from datetime import datetime
from ratelimit import limits, exception
from singer import (
    Transformer,
    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
)


LOGGER = singer.get_logger()


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

    def sync(self):
        """ Sync data from tap source """
        state = self.state
        with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as transformer:
            for stream in self.catalog.get_selected_streams(state):
                LOGGER.info("Syncing stream:" + stream.tap_stream_id)

                singer.write_schema(
                    stream_name=stream.tap_stream_id,
                    schema=stream.schema.to_dict(),
                    key_properties=stream.key_properties,
                )

                for row in self.get_streams(stream.tap_stream_id):
                    record = transformer.transform(
                        row, stream.schema.to_dict(), stream.metadata[0]
                    )

                    singer.write_records(stream.tap_stream_id, [record])

    def get_streams(self, tap_stream_id):
        if tap_stream_id == "advertisables":
            return self.get_advertisables()
        elif tap_stream_id == "campaigns":
            return self.get_campaigns()
        elif tap_stream_id == "deliveries":
            return self.get_deliveries()
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

        self.campaigns = [
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
        return json.loads(json.dumps(campaigns), parse_int=str, parse_float=str)

    def get_deliveries(self):
        deliveries = []
        for campaign in self.campaigns:
            campaign_start_date = campaign.get("start_date") or campaign.get(
                "created_date"
            )
            campaign_start_date = self.format_date(campaign_start_date)

            campaign_end_date = campaign["end_date"]
            if campaign_end_date:
                campaign_end_date = self.format_date(campaign_end_date)
            else:
                if not campaign["is_active"]:
                    campaign_end_date = self.format_date(campaign["updated_date"])
                else:
                    campaign_end_date = datetime.now().strftime("%Y-%m-%d")

            api_result = self.call_api(
                url="uhura/v1/deliveries/campaign",
                params={
                    "breakdowns": "summary",
                    "currency": "USD",
                    "advertisable_eid": campaign["advertisable"],
                    "campaign_eids": campaign["eid"],
                    "start_date": campaign_start_date,
                    "end_date": campaign_end_date,
                },
            )
            summary = api_result["summary"]
            deliveries.append(
                {
                    "campaign_eid": campaign["eid"],
                    "advertisable_eid": campaign["advertisable"],
                    **summary,
                }
            )
        return deliveries

    def format_date(self, input_date):
        return datetime.strptime(input_date, "%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d")

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, exception.RateLimitException),
        max_tries=10,
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
