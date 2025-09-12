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
        th.Property("eventname", th.StringType),
        th.Property("eventtitle", th.StringType),
        th.Property("eventtime", th.StringType),
        th.Property("goodafter", th.StringType),
        th.Property("goodtill", th.StringType),
        th.Property("archivestart", th.StringType),
        th.Property("archiveend", th.StringType),
        th.Property("livestart", th.StringType),
        th.Property("liveend", th.StringType),
    th.Property("isactive", th.StringType),
        th.Property("iseliteexpired", th.StringType),
    th.Property("ishybrideevent", th.StringType),
        th.Property("regrequired", th.StringType),  # Changed to StringType
    th.Property("regnotificationrequired", th.StringType),
        th.Property("description", th.StringType),
        th.Property("promotionalsummary", th.StringType),
        th.Property("displaytimezonecd", th.StringType),
        th.Property("eventtype", th.StringType),
        th.Property("category", th.StringType),
        th.Property("industry", th.StringType),
        th.Property("keyword", th.StringType),
        th.Property("createdby", th.StringType),
        th.Property("createtimestamp", th.StringType),
        th.Property("lastmodified", th.StringType),
        th.Property("lastupdated", th.StringType),
        th.Property("eventduration", th.IntegerType),
        th.Property("eventengagementscore", th.IntegerType),
        th.Property("liveattendees", th.IntegerType),
        th.Property("ondemandattendees", th.IntegerType),
        th.Property("attendedcount", th.IntegerType),
        th.Property("attendedlivecount", th.IntegerType),
        th.Property("attendedarchivecount", th.IntegerType),
        th.Property("attendedwhodownloadedresource", th.IntegerType),
        th.Property("averagearchiveminutes", th.IntegerType),
        th.Property("averagecumulativearchiveminutes", th.IntegerType),
        th.Property("averagecumulativeliveminutes", th.IntegerType),
        th.Property("noshowcount", th.IntegerType),
        th.Property("registrantcount", th.IntegerType),
        th.Property("registrationpagehits", th.IntegerType),
        th.Property("pageviews", th.IntegerType),
        th.Property("eventlocation", th.StringType),
        th.Property("audienceurl", th.StringType),
        th.Property("contenttype", th.StringType),
        th.Property("campaigncode", th.StringType),
        th.Property("partnerref_stats", th.ArrayType(th.ObjectType(
            th.Property("code", th.StringType),
            th.Property("count", th.IntegerType),
        ))),
        th.Property("pmurl", th.StringType),
        th.Property("previewurl", th.StringType),
        th.Property("reporturl", th.StringType),
        th.Property("uploadurl", th.StringType),
        th.Property("singlefilearchive", th.StringType),
        th.Property("presentermediafile", th.StringType),
        th.Property("surveyurls", th.StringType),
        th.Property("tags", th.ArrayType(th.StringType)),
        th.Property("speakers", th.ArrayType(th.ObjectType(
            th.Property("name", th.StringType),
            th.Property("title", th.StringType),
            th.Property("company", th.StringType),
            th.Property("description", th.StringType),
        ))),
        th.Property("sponsor", th.StringType),
        th.Property("streamtype", th.StringType),
        th.Property("totalattendees", th.IntegerType),
        th.Property("totalregistrants", th.IntegerType),
        th.Property("uniqueattendeeresourcedownloads", th.IntegerType),
        th.Property("numberofcontactusrequests", th.IntegerType),
        th.Property("numberofctaclicks", th.IntegerType),
        th.Property("numberofdemoconversions", th.IntegerType),
        th.Property("numberofdrifts", th.IntegerType),
        th.Property("numberoffreetrialrequests", th.IntegerType),
        th.Property("numberofgetpricingrequests", th.IntegerType),
        th.Property("numberofgroupchatmessagessubmitted", th.IntegerType),
        th.Property("numberofmeetingconversions", th.IntegerType),
        th.Property("numberofpollresponses", th.IntegerType),
        th.Property("numberofpollspushed", th.IntegerType),
        th.Property("numberofquestionsanswered", th.IntegerType),
        th.Property("numberofquestionsasked", th.IntegerType),
        th.Property("numberofresourcesavailable", th.IntegerType),
        th.Property("numberofsurveyresponses", th.IntegerType),
        th.Property("numberofsurveyspresented", th.IntegerType),
        th.Property("numberoftestspassed", th.IntegerType),
        th.Property("numberoftestssubmitted", th.IntegerType),
        th.Property("sharethiswidgettotalviews", th.IntegerType),
        th.Property("sharethiswidgetuniqueviews", th.IntegerType),
        th.Property("twitterwidgettotalviews", th.IntegerType),
        th.Property("twitterwidgetuniqueviews", th.IntegerType),
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
        boolean_fields = [
            "isactive", "iseliteexpired", "ishybrideevent", "regrequired", "regnotificationrequired"
        ]
        while True:
            data = self.client.get_events(start_date, items_per_page, page_offset)
            events = data.get("events", [])
            for event in events:
                # Normalize boolean-like fields to strings
                for field in boolean_fields:
                    if field in event and isinstance(event[field], bool):
                        event[field] = str(event[field])
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
        # Get all eventids from the events stream
        events_stream = self._tap.streams["events"]
        for event in events_stream.get_records(context):
            eventid = event["eventid"]
            items_per_page = int(self.config.get("items_per_page", 100))
            page_offset = 0
            import logging
            import time
            MAX_RETRIES = 3
            while True:
                retries = 0
                while retries < MAX_RETRIES:
                    try:
                        data = self.client.get_attendees(eventid, items_per_page, page_offset)
                        break
                    except Exception as e:
                        # Handle 403 Forbidden
                        if hasattr(e, 'response') and getattr(e.response, 'status_code', None) == 403:
                            logging.warning(f"403 Forbidden for event {eventid} page {page_offset}, stopping pagination.")
                            return
                        # Handle connection errors
                        elif 'Connection aborted' in str(e) or 'RemoteDisconnected' in str(e):
                            retries += 1
                            logging.warning(f"Connection error for event {eventid} page {page_offset}, retry {retries}/{MAX_RETRIES}.")
                            time.sleep(2 * retries)
                        else:
                            logging.error(f"Unexpected error for event {eventid} page {page_offset}: {e}")
                            return
                else:
                    logging.error(f"Max retries exceeded for event {eventid} page {page_offset}, skipping page.")
                    break
                attendees = data.get("attendees", [])
                if not attendees:
                    break
                for attendee in attendees:
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
                # Continue paginating until an empty page is returned
                page_offset += 1

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
        events_stream = self._tap.streams["events"]
        for event in events_stream.get_records(context):
            eventid = event["eventid"]
            items_per_page = int(self.config.get("items_per_page", 100))
            page_offset = 0
            import logging
            import time
            MAX_RETRIES = 3
            while True:
                retries = 0
                while retries < MAX_RETRIES:
                    try:
                        data = self.client.get_registrants(eventid, items_per_page, page_offset)
                        break
                    except Exception as e:
                        # Handle 403 Forbidden
                        if hasattr(e, 'response') and getattr(e.response, 'status_code', None) == 403:
                            logging.warning(f"403 Forbidden for event {eventid} page {page_offset}, stopping pagination.")
                            return
                        # Handle connection errors
                        elif 'Connection aborted' in str(e) or 'RemoteDisconnected' in str(e):
                            retries += 1
                            logging.warning(f"Connection error for event {eventid} page {page_offset}, retry {retries}/{MAX_RETRIES}.")
                            time.sleep(2 * retries)
                        else:
                            logging.error(f"Unexpected error for event {eventid} page {page_offset}: {e}")
                            return
                else:
                    logging.error(f"Max retries exceeded for event {eventid} page {page_offset}, skipping page.")
                    break
                registrants = data.get("registrants", [])
                if not registrants:
                    break
                for registrant in registrants:
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
                                elif isinstance(v, list):
                                    for item in v:
                                        cast_ids(item)
                                elif isinstance(v, dict):
                                    cast_ids(v)
                        elif isinstance(obj, list):
                            for item in obj:
                                cast_ids(item)

                    cast_ids(registrant)
                    yield registrant
                # Continue paginating until an empty page is returned
                page_offset += 1