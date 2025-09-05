# tap-on24

`tap-on24` is a Singer tap designed for extracting event data from the ON24 Webinar Platform using their REST API.

This tap is built using the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

---

## Installation

Install the tap using the following command:

```bash
pipx install git+https://github.com/The-Daily-Upside/tap-on24.git
```

---

## Configuration

### Supported Configuration Options

The following configuration options are supported:

* **`client_id`**: Your ON24 client ID.
* **`access_token_key`**: Your ON24 API access token key.
* **`access_token_secret`**: Your ON24 API access token secret.
* **`start_date`**: (Optional) Start date for event filtering (YYYY-MM-DD).
* **`end_date`**: (Optional) End date for event filtering (YYYY-MM-DD).
* **`items_per_page`**: (Optional) Number of events per page (default: 100).

### Example meltano.yml

```yaml
plugins:
  extractors:
    - name: tap-on24
      namespace: tap_on24
      executable: tap-on24
      settings:
        client_id: "YOUR_CLIENT_ID"
        access_token_key: "YOUR_ACCESS_TOKEN_KEY"
        access_token_secret: "YOUR_ACCESS_TOKEN_SECRET"
        start_date: "2025-01-01"
        end_date: "2025-09-01"
        items_per_page: 100
```

---

## Usage

Run the tap to extract events from ON24:

```bash
meltano run tap-on24
```

---

## Output

The tap will output ON24 event records, matching the ON24 event API schema.

---

## Authentication

Authentication is handled via custom HTTP headers:

* `accessTokenKey`: Your ON24 API access token key
* `accessTokenSecret`: Your ON24 API access token secret

The `client_id` is included in the API request URL path.

### Example Configuration File

```json
{
  "service_account_key_file": "/path/to/your-service-account-key.json",
  "network_id": "your-network-id",
  "reports": {
    "line_items_last_year": {
      "displayName": "line_items_last_year",
      "reportDefinition": {
        "dimensions": [
          "DATE",
          "ORDER_ID",
          "ORDER_NAME",
          "LINE_ITEM_ID",
          "LINE_ITEM_NAME"
        ],
        "metrics": [
          "IMPRESSIONS",
          "CLICKS",
          "CTR"
        ],
        "dateRange": {
          "relative": "LAST_365_DAYS"
        },
        "reportType": "HISTORICAL"
      }
    },
    "creatives_last_year": {
      "displayName": "creatives_last_year",
      "reportDefinition": {
        "dimensions": [
          "DATE",
          "ORDER_ID",
          "ORDER_NAME",
          "LINE_ITEM_ID",
          "LINE_ITEM_NAME",
          "CREATIVE_ID",
          "CREATIVE_NAME",
          "CREATIVE_TYPE",
          "CREATIVE_TYPE_NAME",
          "RENDERED_CREATIVE_SIZE"
        ],
        "metrics": [
          "IMPRESSIONS",
          "CLICKS",
          "CTR"
        ],
        "dateRange": {
          "relative": "LAST_365_DAYS"
        },
        "reportType": "HISTORICAL"
      }
    }
  }
}
```

To view the full list of supported settings and capabilities, run:

```bash
tap-google-ad-manager --about
```

---

## Usage

### Running the Tap

You can execute the tap directly using the following commands:

```bash
tap-google-ad-manager --version
tap-google-ad-manager --help
tap-google-ad-manager --config config.json --discover > catalog.json
tap-google-ad-manager --config config.json --catalog catalog.json
```

#### Example Reports:

* **Line Items Last Year**: Includes impressions, clicks, and CTR over the past year.
* **Creatives Last Year**: Includes creative metadata and metrics over the past year.

#### Logs

The tap provides detailed logs for report creation, execution, and data fetching. Look for logs like:

* `📡 [ensure_reports_exist] GET ...`
* `🏃 [run_report] POST ...`
* `📥 Fetch rows body: ...`

---

### Key Files and Directories

* **`tap_google_ad_manager/`**: Contains the main implementation of the tap, including:

  * `client.py`: Handles API requests and authentication.
  * `streams.py`: Defines the data streams for Google Ad Manager resources, including reports.
  * `tap.py`: Entry point for the tap.
* **`meltano.yml`**: Configuration file for Meltano integration.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
