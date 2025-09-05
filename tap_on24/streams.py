from typing import Any, Dict, Optional, Iterable
from singer_sdk import typing as th
from tap_on24.client import ON24Client

class ON24EventsStream:
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
        self.tap = tap
        self.client = ON24Client(
            tap.config.get("client_id"),
            tap.config.get("access_token_key"),
            tap.config.get("access_token_secret")
        )

    def get_records(self) -> Iterable[Dict[str, Any]]:
        start_date = self.tap.config.get("start_date")
        end_date = self.tap.config.get("end_date")
        items_per_page = int(self.tap.config.get("items_per_page", 100))
        page_offset = 0
        while True:
            data = self.client.get_events(start_date, end_date, items_per_page, page_offset)
            events = data.get("events", [])
            for event in events:
                yield event
            if len(events) < items_per_page:
                break
            page_offset += 1

# All legacy classes and code below this line are removed for ON24 tap

# All legacy classes and code below this line are removed for ON24 tap

# All legacy classes and code below this line are removed for ON24 tap

# All legacy classes and code below this line are removed for ON24 tap




class PlacementsStream(BaseSimpleStream):
    name = "placements"
    path = "networks/{network_id}/placements"
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("placementId", th.StringType),
        th.Property("displayName", th.StringType),
        th.Property("description", th.StringType),
        th.Property("targetingDescription", th.StringType),
        th.Property("adUnits", th.ArrayType(th.StringType)),
        th.Property("status", th.StringType),
        th.Property("appliedTeams", th.ArrayType(th.StringType)),
        th.Property("updateTime", th.DateTimeType)
    ).to_dict()


class ReportsStream(BaseSimpleStream):
    name = "reports"
    path = "networks/{network_id}/reports"
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("reportId", th.StringType),
        th.Property("displayName", th.StringType),
        th.Property("description", th.StringType),
        th.Property("dimensions", th.ArrayType(th.StringType)),
        th.Property("metrics", th.ArrayType(th.StringType)),
        th.Property("filters", th.ArrayType(th.ObjectType())),
        th.Property("updateTime", th.DateTimeType)
    ).to_dict()

class ReportResultsStream(GoogleAdManagerStream):
    name = "report_results"
    path = ""
    primary_keys = ["result_name"]
    replication_key = None
    # Treat rows as raw JSON string to preserve all fields
    schema = th.PropertiesList(
        th.Property("result_name", th.StringType),
        th.Property("report_id", th.StringType),
        th.Property("report_name", th.StringType),
        th.Property("report_display_name", th.StringType),
        th.Property("report_definition", th.StringType),
        th.Property("report_definition_dimensions", th.StringType),
        th.Property("report_definition_metrics", th.StringType),
        th.Property("report_results_dimension", th.StringType),
        th.Property("report_results_metrics", th.StringType),
        th.Property("run_time", th.DateTimeType),
    ).to_dict()

    def __init__(self, tap, *args, **kwargs):
        super().__init__(tap, *args, **kwargs)
        self.tap = tap

    def ensure_reports_exist(self, network_id: str, reports: Dict[str, dict]):
        reports_url = f"{self.url_base}networks/{network_id}/reports"
        self.logger.info(f"📡 [ensure_reports_exist] GET {reports_url}")

        def fetch_reports():
            self.logger.info("🔍 Fetching existing reports...")
            resp = self.request_decorator(requests.get)(reports_url, headers=self.http_headers)
            self.logger.info(f"🔁 Fetch reports status: {resp.status_code}")
            self.logger.info(f"📥 Fetch reports body: {resp.text}")
            if resp.status_code != 200:
                self.logger.error("❌ Failed to fetch existing reports.")
                return {}
            return {r.get("displayName"): r.get("reportId") for r in resp.json().get("reports", [])}

        report_map = fetch_reports()
        self.logger.info(f"🧾 Existing report map: {report_map}")

        for name, spec in self.config.get("reports", {}).items():
            if name not in report_map:
                self.logger.info(f"🆕 Creating report: {name}")
                create_resp = self.request_decorator(requests.post)(
                    reports_url,
                    headers=self.http_headers,
                    json={"displayName": name, **spec}
                )
                self.logger.info(f"📤 Create report status: {create_resp.status_code}")
                self.logger.info(f"📥 Create report body: {create_resp.text}")
                if create_resp.status_code != 200:
                    self.logger.error(f"❌ Failed to create report: {name}")
                    continue
                for attempt in range(MAX_RETRIES):
                    self.logger.info(f"⏱️ Waiting for report '{name}' (attempt {attempt+1})...")
                    time.sleep(RETRY_DELAY)
                    report_map = fetch_reports()
                    if name in report_map:
                        self.logger.info(f"✅ Report now exists: {name}")
                        break
        self.report_display_name_to_id = report_map

    def run_report(self, report_name: str) -> str:
        url = f"{self.url_base}{report_name}:run"
        self.logger.info(f"🏃 [run_report] POST {url}")
        resp = self.request_decorator(requests.post)(url, headers=self.http_headers)
        self.logger.info(f"📤 Run report status: {resp.status_code}")
        self.logger.info(f"📥 Run report body: {resp.text}")
        try:
            return resp.json().get("name")
        except JSONDecodeError:
            raise RuntimeError("❌ Failed to decode run_report response.")

    def wait_for_completion(self, operation_name: str, poll_interval: float = 5.0, timeout: float = 300.0) -> dict:
        url = f"{self.url_base}{operation_name}"
        self.logger.info(f"📡 [wait_for_completion] Polling {url}")
        start = time.time()
        while time.time() - start < timeout:
            resp = self.request_decorator(requests.get)(url, headers=self.http_headers)
            self.logger.info(f"📤 Poll status: {resp.status_code}")
            self.logger.info(f"📥 Poll body: {resp.text}")
            try:
                data = resp.json()
            except JSONDecodeError:
                raise RuntimeError("❌ Failed to decode poll response.")
            if data.get("done"):
                self.logger.info(f"✅ Report completed: {data}")
                if data.get("error"):
                    raise RuntimeError(f"❌ Report error: {data['error']}")
                return data.get("response", {})
            self.logger.info("⏳ Report still running...")
            time.sleep(poll_interval)
        raise TimeoutError("⌛ Timeout: report operation did not complete in time.")

    def fetch_all_rows(self, result_name: str) -> list:
        all_rows: list = []
        page_token: Optional[str] = None
        while True:
            url = f"{self.url_base}{result_name}:fetchRows"
            params = {"pageSize": 10000, **({"pageToken": page_token} if page_token else {})}
            self.logger.info(f"📡 [fetch_rows] GET {url} with params: {params}")
            resp = self.request_decorator(requests.get)(url, headers=self.http_headers, params=params)
            self.logger.info(f"📤 Fetch rows status: {resp.status_code}")
            body_preview = resp.text[:500].replace("\n", " ")
            self.logger.info(f"📥 Fetch rows body: {body_preview}...")
            try:
                data = resp.json()
            except JSONDecodeError:
                raise RuntimeError(f"❌ Invalid JSON when fetching rows: {resp.text}")
            all_rows.extend(data.get("rows", []))
            page_token = data.get("nextPageToken")
            if not page_token:
                break
        return all_rows

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        self.logger.info("🚦 Starting ReportResultsStream.get_records()")
        network_id = self.config.get("network_id")
        if not network_id:
            raise ValueError("Missing required config value: 'network_id'")
        reports_cfg = self.config.get("reports") or {}
        self.ensure_reports_exist(network_id, reports_cfg)
        for name, _ in reports_cfg.items():
            report_id = self.report_display_name_to_id.get(name)
            if not report_id:
                self.logger.warning(f"⚠️ Skipping report '{name}': No ID found.")
                continue
            report_name = f"networks/{network_id}/reports/{report_id}"
            self.logger.info(f"📊 Processing report: {report_name}")
            try:
                operation_name = self.run_report(report_name)
                self.logger.info(f"📡 Operation: {operation_name}")
                response = self.wait_for_completion(operation_name)
                result_name = response.get("reportResult")
                self.logger.info(f"📁 Result name: {result_name}")
                if not result_name:
                    self.logger.warning(f"⚠️ No result returned for report {report_name}")
                    continue
                rows = self.fetch_all_rows(result_name)
                valid_rows = [r for r in rows if isinstance(r, dict)]
                report_results_dimension = json.dumps([
                    r.get("dimensionValues") for r in valid_rows if "dimensionValues" in r
                ])
                report_results_metrics = json.dumps([
                    r.get("metricValueGroups") for r in valid_rows if "metricValueGroups" in r
                ])
                for idx, r in enumerate(rows):
                    if not isinstance(r, dict):
                        self.logger.warning(f"⚠️ Row {idx} is not a dict: {r}")
                yield {
                    "result_name": result_name,
                    "report_id": report_id,
                    "report_name": report_name,
                    "report_display_name": name,
                    "report_definition": json.dumps(self.config.get("reports").get(name).get("reportDefinition")),
                    "report_definition_dimensions": json.dumps(
                        self.config.get("reports").get(name).get("reportDefinition").get("dimensions")
                    ),
                    "report_definition_metrics": json.dumps(
                        self.config.get("reports").get(name).get("reportDefinition").get("metrics")
                    ),
                    "report_results_dimension": report_results_dimension,
                    "report_results_metrics": report_results_metrics,
                    "run_time": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                }

            except Exception as e:
                self.logger.error(f"❌ Failed to process report {report_name}: {e}")
