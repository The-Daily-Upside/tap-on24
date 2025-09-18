from typing import Any, Dict, Optional, Iterable
from singer_sdk import typing as th
from singer_sdk.streams import Stream
from tap_on24.client import ON24Client
import logging
import time

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
        logging.debug(f"[ON24EventsStream] Starting event fetch: start_date={start_date}, items_per_page={items_per_page}")
        while True:
            logging.debug(f"[ON24EventsStream] Fetching events page_offset={page_offset}")
            data = self.client.get_events(start_date, items_per_page, page_offset)
            events = data.get("events", [])
            logging.debug(f"[ON24EventsStream] Got {len(events)} events on page {page_offset}")
            for idx, event in enumerate(events):
                logging.debug(f"[ON24EventsStream] Yielding event {idx+1} on page {page_offset}: eventid={event.get('eventid')}")
                yield event
            if len(events) < items_per_page:
                logging.debug(f"[ON24EventsStream] Last page reached at page_offset={page_offset}")
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
        # Get all eventids from the events stream
        events_stream = self._tap.streams["events"]
        logging.debug("[ON24AttendeesStream] Starting attendee extraction for all events.")
        for event_idx, event in enumerate(events_stream.get_records(context)):
            eventid = event["eventid"]
            logging.debug(f"[ON24AttendeesStream] Processing event {event_idx+1}: eventid={eventid}")
            items_per_page = int(self.config.get("items_per_page", 100))
            page_offset = 0
            MAX_RETRIES = 3
            while True:
                retries = 0
                while retries < MAX_RETRIES:
                    try:
                        logging.debug(f"[ON24AttendeesStream] Fetching attendees for eventid={eventid}, page_offset={page_offset}")
                        data = self.client.get_attendees(eventid, items_per_page, page_offset)
                        break
                    except Exception as e:
                        # Handle 403 Forbidden
                        if hasattr(e, 'response') and getattr(e.response, 'status_code', None) == 403:
                            logging.warning(f"403 Forbidden for event {eventid} page {page_offset}, skipping event.")
                            return  # Immediately skip to next event
                        # Handle connection errors
                        elif 'Connection aborted' in str(e) or 'RemoteDisconnected' in str(e):
                            retries += 1
                            logging.warning(f"Connection error for event {eventid} page {page_offset}, retry {retries}/{MAX_RETRIES}.")
                            time.sleep(2 * retries)
                        else:
                            logging.error(f"Unexpected error for event {eventid} page {page_offset}: {e}")
                            break
                else:
                    logging.error(f"Max retries exceeded for event {eventid} page {page_offset}, skipping page.")
                    break
                attendees = data.get("attendees", [])
                logging.debug(f"[ON24AttendeesStream] Got {len(attendees)} attendees for eventid={eventid} on page {page_offset}")
                if not attendees:
                    logging.debug(f"[ON24AttendeesStream] No more attendees for eventid={eventid} at page_offset={page_offset}")
                    break  # No more attendees, stop paginating for this event
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
                    logging.debug(f"[ON24AttendeesStream] Yielding attendee {att_idx+1} for eventid={eventid} on page {page_offset}: eventuserid={attendee.get('eventuserid')}")
                    yield attendee
                # Continue paginating until an empty page is returned
                page_offset += 1
