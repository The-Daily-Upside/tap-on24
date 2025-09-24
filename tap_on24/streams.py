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
        th.Property("eventanalytics", th.ObjectType(
            th.Property("totalregistrants", th.IntegerType),
            th.Property("totalattendees", th.IntegerType),
            th.Property("noshowcount", th.IntegerType),
            th.Property("registrationpagehits", th.IntegerType),
            th.Property("numberofgetpricingrequests", th.IntegerType),
            th.Property("numberoffreetrialrequests", th.IntegerType),
            th.Property("numberofresourcesavailable", th.IntegerType),
            th.Property("attendeeswhodownloadedresource", th.IntegerType),
            th.Property("uniqueattendeeresourcedownloads", th.IntegerType),
            th.Property("numberofmeetingconversions", th.IntegerType),
            th.Property("numberofdemoconversions", th.IntegerType),
            th.Property("averagearchiveminutes", th.IntegerType),
            th.Property("averagecumulativearchiveminutes", th.IntegerType),
            th.Property("totalcumulativeliveminutes", th.IntegerType),
            th.Property("totalcumulativearchiveminutes", th.IntegerType),
            th.Property("totalcumulativeminutes", th.IntegerType),
            th.Property("totalmediaplayerminutes", th.IntegerType),
            th.Property("totallivemediaplayerminutes", th.IntegerType),
            th.Property("totalarchivemediaplayerminutes", th.IntegerType),
            th.Property("liveattendees", th.IntegerType),
            th.Property("ondemandattendees", th.IntegerType),
            th.Property("averageliveminutes", th.IntegerType),
            th.Property("averagecumulativeliveminutes", th.IntegerType),
        )),
        th.Property("scheduledeventduration", th.IntegerType),
        th.Property("eventstd1", th.StringType),
        th.Property("eventstd2", th.StringType),
        th.Property("eventstd3", th.StringType),
        th.Property("eventstd4", th.StringType),
        th.Property("eventstd5", th.StringType),
        th.Property("tags", th.ArrayType(th.StringType)),
        th.Property("funnelstages", th.ArrayType(th.StringType)),
        th.Property("speakers", th.ArrayType(th.ObjectType(
            th.Property("name", th.StringType),
            th.Property("title", th.StringType),
            th.Property("company", th.StringType),
            th.Property("description", th.StringType),
        ))),
        th.Property("partnerrefstats", th.ArrayType(th.ObjectType(
            th.Property("code", th.StringType),
            th.Property("count", th.IntegerType),
        ))),
        th.Property("surveyurls", th.ArrayType(th.StringType)),
        th.Property("customaccounttags", th.ArrayType(th.ObjectType(
            th.Property("groupid", th.IntegerType),
            th.Property("groupname", th.StringType),
            th.Property("tagid", th.IntegerType),
            th.Property("tagname", th.StringType),
        ))),
        th.Property("customeventfields", th.ArrayType(th.ObjectType(
            th.Property("name", th.StringType),
            th.Property("label", th.StringType),
            th.Property("value", th.StringType),
        ))),
        th.Property("media", th.ObjectType(
            th.Property("audios", th.ArrayType(th.StringType)),
            th.Property("videos", th.ArrayType(th.StringType)),
            th.Property("slides", th.ArrayType(th.StringType)),
            th.Property("videoclips", th.ArrayType(th.StringType)),
            th.Property("urls", th.ArrayType(th.StringType)),
            th.Property("polls", th.ArrayType(th.StringType)),
        )),
        th.Property("categories", th.ArrayType(th.StringType)),
        th.Property("tracks", th.ArrayType(th.StringType)),
        th.Property("livedays", th.ArrayType(th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("title", th.StringType),
            th.Property("livestarttime", th.StringType),
            th.Property("liveendtime", th.StringType),
        ))),
        th.Property("contents", th.ArrayType(th.ObjectType(
            th.Property("title", th.StringType),
            th.Property("type", th.StringType),
            th.Property("status", th.StringType),
            th.Property("resourceid", th.IntegerType),
            th.Property("externalurl", th.StringType),
        ))),
        th.Property("sponsors", th.ArrayType(th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
            th.Property("staff", th.ArrayType(th.ObjectType(
                th.Property("firstname", th.StringType),
                th.Property("lastname", th.StringType),
                th.Property("company", th.StringType),
                th.Property("title", th.StringType),
                th.Property("email", th.StringType),
                th.Property("roles", th.ArrayType(th.StringType)),
            ))),
        ))),
        th.Property("encoders", th.ArrayType(th.ObjectType(
            th.Property("encoder", th.StringType),
            th.Property("url", th.StringType),
            th.Property("streamid", th.StringType),
        ))),
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
            for idx, event in enumerate(events):
                # Ensure 'lastupdated' is present for replication key
                if "lastupdated" not in event:
                    event["lastupdated"] = None
                yield event
            if len(events) < items_per_page:
                break
            page_offset += 1

class ON24AttendeesStream(Stream):
    name = "attendees"
    primary_keys = ["eventid", "eventuserid"]
    schema = th.PropertiesList(
        th.Property("eventid", th.IntegerType),
        th.Property("email", th.StringType),
        th.Property("eventuserid", th.IntegerType),
        th.Property("exteventusercd", th.StringType),
        th.Property("userstatus", th.StringType)
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.client = ON24Client(
            tap.config.get("client_id"),
            tap.config.get("access_token_key"),
            tap.config.get("access_token_secret")
        )

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        import logging
        # Get all eventids from the events stream
        events_stream = self._tap.streams["events"]
        for event_idx, event in enumerate(events_stream.get_records(context)):
            eventid = event["eventid"]
            items_per_page = int(self.config.get("items_per_page", 100))
            page_offset = 0
            total_attendees = None
            while True:
                data = self.client.get_attendees(eventid, items_per_page, page_offset)
                attendees = data.get("attendees", [])
                if total_attendees is None:
                    total_attendees = data.get("totalattendees")
                if not attendees:
                    break
                for att_idx, attendee in enumerate(attendees):
                    attendee["eventid"] = int(eventid)
                    if "eventuserid" in attendee and attendee["eventuserid"] is not None:
                        try:
                            attendee["eventuserid"] = int(attendee["eventuserid"])
                        except (ValueError, TypeError):
                            attendee["eventuserid"] = None

                    def cast_ids(obj):
                        if isinstance(obj, dict):
                            for k, v in obj.items():
                                # Cast *_id fields to int
                                if k.endswith("id") and v is not None:
                                    try:
                                        obj[k] = int(v)
                                    except (ValueError, TypeError):
                                        obj[k] = None
                                # Recurse into lists and dicts, but do NOT convert pollanswers/surveyanswers
                                elif isinstance(v, list) and k not in ("pollanswers", "surveyanswers"):
                                    for item in v:
                                        cast_ids(item)
                                elif isinstance(v, dict):
                                    cast_ids(v)
                        elif isinstance(obj, list):
                            for item in obj:
                                cast_ids(item)

                    # Cast *_id fields to int, preserve pollanswers/surveyanswers as arrays
                    cast_ids(attendee)
                    yield attendee
                page_offset += 1
                # Stop if we've fetched all attendees
                if total_attendees is not None and (page_offset * items_per_page) >= total_attendees:
                    break

class ON24RegistrantsStream(Stream):
    name = "registrants"
    primary_keys = ["eventid", "eventuserid"]
    schema = th.PropertiesList(
        th.Property("eventid", th.IntegerType),
        th.Property("eventuserid", th.IntegerType),
        th.Property("firstname", th.StringType),
        th.Property("lastname", th.StringType),
        th.Property("email", th.StringType),
        th.Property("company", th.StringType),
        th.Property("jobtitle", th.StringType),
        th.Property("addressstreet1", th.StringType),
        th.Property("addressstreet2", th.StringType),
        th.Property("city", th.StringType),
        th.Property("state", th.StringType),
        th.Property("zip", th.StringType),
        th.Property("country", th.StringType),
        th.Property("workphone", th.StringType),
        th.Property("jobfunction", th.StringType),
        th.Property("companyindustry", th.StringType),
        th.Property("companysize", th.StringType),
        th.Property("partnerref", th.StringType),
        th.Property("std1", th.StringType),
        th.Property("std2", th.StringType),
        th.Property("std3", th.StringType),
        th.Property("std4", th.StringType),
        th.Property("std5", th.StringType),
        th.Property("std6", th.StringType),
        th.Property("std7", th.StringType),
        th.Property("std8", th.StringType),
        th.Property("std9", th.StringType),
        th.Property("std10", th.StringType),
        th.Property("fax", th.StringType),
        th.Property("username", th.StringType),
        th.Property("exteventusercd", th.StringType),
        th.Property("other", th.StringType),
        th.Property("notes", th.StringType),
        th.Property("marketingemail", th.StringType),
        th.Property("eventemail", th.StringType),
        th.Property("homephone", th.StringType),
        th.Property("createtimestamp", th.StringType),
        th.Property("lastactivity", th.StringType),
        th.Property("browser", th.StringType),
        th.Property("ipaddress", th.StringType),
        th.Property("os", th.StringType),
        th.Property("emailformat", th.StringType),
        th.Property("engagementprediction", th.StringType),
        th.Property("userprofileurl", th.StringType),
        th.Property("campaigncode", th.StringType),
        th.Property("sourcecampaigncode", th.StringType),
        th.Property("sourceeventid", th.IntegerType),
        th.Property("userstatus", th.StringType),
        th.Property("utmsource", th.StringType),
        th.Property("utmmedium", th.StringType),
        th.Property("utmcampaign", th.StringType),
        th.Property("utmterm", th.StringType),
        th.Property("utmcontent", th.StringType),
        th.Property("attendeetype", th.StringType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.client = ON24Client(
            tap.config.get("client_id"),
            tap.config.get("access_token_key"),
            tap.config.get("access_token_secret")
        )

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        import logging
        # Get all eventids from the events stream
        events_stream = self._tap.streams["events"]
        for event_idx, event in enumerate(events_stream.get_records(context)):
            eventid = event["eventid"]
            items_per_page = int(self.config.get("items_per_page", 100))
            page_offset = 0
            total_registrants = None
            while True:
                data = self.client.get_registrants(eventid, items_per_page, page_offset)
                registrants = data.get("registrants", [])
                if total_registrants is None:
                    total_registrants = data.get("totalregistrants")
                if not registrants:
                    break
                for att_idx, registrant in enumerate(registrants):
                    registrant["eventid"] = int(eventid)
                    if "eventuserid" in registrant and registrant["eventuserid"] is not None:
                        try:
                            registrant["eventuserid"] = int(registrant["eventuserid"])
                        except (ValueError, TypeError):
                            registrant["eventuserid"] = None

                    def cast_ids(obj):
                        if isinstance(obj, dict):
                            for k, v in obj.items():
                                # Cast *_id fields to int
                                if k.endswith("id") and v is not None:
                                    try:
                                        obj[k] = int(v)
                                    except (ValueError, TypeError):
                                        obj[k] = None
                                # Recurse into lists and dicts, but do NOT convert pollanswers/surveyanswers
                                elif isinstance(v, list) and k not in ("pollanswers", "surveyanswers"):
                                    for item in v:
                                        cast_ids(item)
                                elif isinstance(v, dict):
                                    cast_ids(v)
                        elif isinstance(obj, list):
                            for item in obj:
                                cast_ids(item)

                    # Cast *_id fields to int, preserve pollanswers/surveyanswers as arrays
                    cast_ids(registrant)
                    yield registrant
                page_offset += 1
                # Stop if we've fetched all registrants
                if total_registrants is not None and (page_offset * items_per_page) >= total_registrants:
                    break
