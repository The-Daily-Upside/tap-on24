
from typing import Any, Dict, Optional, Iterable
from singer_sdk import typing as th
from singer_sdk.streams import Stream
from tap_on24.client import ON24Client

class ON24EventsStream(Stream):
    name = "events"
    primary_keys = ["eventid"]
    replication_key = "lastupdated"
    schema = th.PropertiesList(
        th.Property("eventid", th.IntegerType),
        th.Property("clientid", th.IntegerType),
        th.Property("goodafter", th.StringType),
        th.Property("isactive", th.BooleanType),
        th.Property("regrequired", th.BooleanType),
        th.Property("description", th.StringType),
        th.Property("promotionalsummary", th.StringType),
        th.Property("regnotificationrequired", th.BooleanType),
        th.Property("displaytimezonecd", th.StringType),
        th.Property("eventtype", th.StringType),
        th.Property("category", th.StringType),
        th.Property("createtimestamp", th.StringType),
        th.Property("localelanguagecd", th.StringType),
        th.Property("localecountrycd", th.StringType),
        th.Property("lastmodified", th.StringType),
        th.Property("lastupdated", th.StringType),
        th.Property("iseliteexpired", th.StringType),
        th.Property("application", th.StringType),
        th.Property("livestart", th.StringType),
        th.Property("liveend", th.StringType),
        th.Property("archivestart", th.StringType),
        th.Property("archiveend", th.StringType),
        th.Property("audienceurl", th.StringType),
        th.Property("contenttype", th.StringType),
        th.Property("campaigncode", th.StringType),
        th.Property("eventlocation", th.StringType),
        th.Property("createdby", th.StringType),
        th.Property("ishybridevent", th.BooleanType),
        th.Property("istestevent", th.BooleanType),
        th.Property("eventprofile", th.StringType),
        th.Property("streamtype", th.StringType),
        th.Property("audiencekey", th.StringType),
        th.Property("extaudienceurl", th.StringType),
        th.Property("reporturl", th.StringType),
        th.Property("uploadurl", th.StringType),
        th.Property("pmurl", th.StringType),
        th.Property("previewurl", th.StringType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.client = ON24Client(
            tap.config.get("client_id"),
            tap.config.get("access_token_key"),
            tap.config.get("access_token_secret")
        )

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        start_date = self.config.get("on24_start_date")
        items_per_page = int(self.config.get("items_per_page", 100))
        page_offset = 0
        while True:
            data = self.client.get_events(start_date, items_per_page, page_offset)
            events = data.get("events", [])
            for event in events:
                yield event
            if len(events) < items_per_page:
                break
            page_offset += 1
