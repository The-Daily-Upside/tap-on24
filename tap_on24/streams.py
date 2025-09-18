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
        th.Property("description", th.StringType),
        th.Property("lastupdated", th.StringType),   
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
        th.Property("userstatus", th.StringType),
        th.Property("isblocked", th.StringType),
        th.Property("engagementscore", th.NumberType),
        th.Property("liveminutes", th.IntegerType),
        th.Property("liveviewed", th.IntegerType),
        th.Property("firstliveactivity", th.StringType),
        th.Property("lastliveactivity", th.StringType),
        th.Property("archiveminutes", th.IntegerType),
        th.Property("archiveviewed", th.IntegerType),
        th.Property("firstarchiveactivity", th.StringType),
        th.Property("lastarchiveactivity", th.StringType),
        th.Property("askedquestions", th.IntegerType),
        th.Property("resourcesdownloaded", th.IntegerType),
        th.Property("answeredpolls", th.IntegerType),
        th.Property("answeredsurveys", th.IntegerType),
        th.Property("answeredsurveyquestions", th.IntegerType),
        th.Property("launchmode", th.StringType),
        th.Property("userprofileurl", th.StringType),
        th.Property("campaigncode", th.StringType),
        th.Property("sourcecampaigncode", th.StringType),
        th.Property("sourceeventid", th.IntegerType),
        th.Property("cumulativeliveminutes", th.IntegerType),
        th.Property("cumulativearchiveminutes", th.IntegerType),
        th.Property("partnerref", th.StringType),
        th.Property("attendancepartnerref", th.StringType),
        th.Property("attendeesessions", th.IntegerType),
        th.Property("livemediaplayerminutes", th.IntegerType),
        th.Property("archivemediaplayerminutes", th.IntegerType),
        th.Property("questions", th.ArrayType(th.ObjectType(
            th.Property("questionid", th.IntegerType),
            th.Property("createtimestamp", th.StringType),
            th.Property("content", th.StringType),
            th.Property("foldername", th.StringType),
        ))),
        th.Property("polls", th.ArrayType(th.ObjectType(
            th.Property("pollid", th.IntegerType),
            th.Property("pollsubmittedtimestamp", th.StringType),
            th.Property("pollquestionid", th.IntegerType),
            th.Property("pollquestion", th.StringType),
            th.Property("pollanswers", th.ArrayType(th.StringType)),
            th.Property("pollanswersdetail", th.ArrayType(th.ObjectType(
                th.Property("answercode", th.StringType),
                th.Property("answer", th.StringType),
            ))),
        ))),
        th.Property("resources", th.ArrayType(th.ObjectType(
            th.Property("resourceid", th.IntegerType),
            th.Property("resourceviewed", th.StringType),
            th.Property("resourceviewedtimestamp", th.StringType),
        ))),
        th.Property("certificationwidgetresult", th.StringType),
        th.Property("certificationcredit", th.StringType),
        th.Property("certificationtimestamp", th.StringType),
        th.Property("certifications", th.ArrayType(th.ObjectType(
            th.Property("certificationid", th.IntegerType),
            th.Property("certificationname", th.StringType),
            th.Property("certificationcredit", th.StringType),
            th.Property("certificationurl", th.StringType),
            th.Property("certificationtimestamp", th.StringType),
            th.Property("certificationresult", th.StringType),
        ))),
        th.Property("democonversions", th.ArrayType(th.ObjectType(
            th.Property("widgetid", th.IntegerType),
            th.Property("widgetname", th.StringType),
            th.Property("widgettype", th.StringType),
            th.Property("widgetaction", th.StringType),
            th.Property("widgetsubmittedtimestamp", th.StringType),
        ))),
        th.Property("meetingconversions", th.ArrayType(th.ObjectType(
            th.Property("widgetid", th.IntegerType),
            th.Property("widgetname", th.StringType),
            th.Property("widgettype", th.StringType),
            th.Property("widgetaction", th.StringType),
            th.Property("widgetsubmittedtimestamp", th.StringType),
        ))),
        th.Property("contactus", th.ArrayType(th.ObjectType(
            th.Property("widgetid", th.IntegerType),
            th.Property("widgetname", th.StringType),
            th.Property("widgettype", th.StringType),
            th.Property("widgetaction", th.StringType),
            th.Property("widgetsubmittedtimestamp", th.StringType),
        ))),
        th.Property("getpricing", th.ArrayType(th.ObjectType(
            th.Property("widgetid", th.IntegerType),
            th.Property("widgetname", th.StringType),
            th.Property("widgettype", th.StringType),
            th.Property("widgetaction", th.StringType),
            th.Property("widgetsubmittedtimestamp", th.StringType),
        ))),
        th.Property("freetrial", th.ArrayType(th.ObjectType(
            th.Property("widgetid", th.IntegerType),
            th.Property("widgetname", th.StringType),
            th.Property("widgettype", th.StringType),
            th.Property("widgetaction", th.StringType),
            th.Property("widgetsubmittedtimestamp", th.StringType),
        ))),
        th.Property("drift", th.ArrayType(th.ObjectType(
            th.Property("widgetid", th.IntegerType),
            th.Property("widgetname", th.StringType),
            th.Property("widgettype", th.StringType),
            th.Property("widgetaction", th.StringType),
            th.Property("widgetsubmittedtimestamp", th.StringType),
        ))),
        th.Property("locationvisits", th.ArrayType(th.ObjectType(
            th.Property("locationid", th.IntegerType),
            th.Property("locationcode", th.StringType),
            th.Property("locationname", th.StringType),
            th.Property("sponsorid", th.IntegerType),
            th.Property("sponsorname", th.StringType),
            th.Property("visits", th.IntegerType),
            th.Property("visitsduration", th.IntegerType),
            th.Property("cumulativevisitsduration", th.IntegerType),
        ))),
        th.Property("surveys", th.ArrayType(th.ObjectType(
            th.Property("surveyid", th.StringType),
            th.Property("surveysubmittedtimestamp", th.StringType),
            th.Property("surveyquestions", th.ArrayType(th.ObjectType(
                th.Property("surveyquestionid", th.IntegerType),
                th.Property("surveyquestion", th.StringType),
                th.Property("questioncode", th.StringType),
                th.Property("primaryquestioncode", th.StringType),
                th.Property("surveyanswers", th.ArrayType(th.StringType)),
                th.Property("surveyanswersdetail", th.ArrayType(th.ObjectType(
                    th.Property("answercode", th.StringType),
                    th.Property("answer", th.StringType),
                ))),
            ))),
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
