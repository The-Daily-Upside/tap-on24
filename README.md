# tap-on24

`tap-on24` is a Singer tap for the ON24 Webinar Platform, built with the Meltano SDK.

**Currently, only the `events` stream is supported.**

---

## Installation

Install the tap using pipx or pip:

```bash
pipx install git+https://github.com/The-Daily-Upside/tap-on24.git
# or
pip install git+https://github.com/The-Daily-Upside/tap-on24.git
```

---


## Configuration

Supported configuration options:

- `client_id`: Your ON24 client ID (required)
- `access_token_key`: Your ON24 API access token key (required)
- `access_token_secret`: Your ON24 API access token secret (required)
- `on24_start_date`: (optional) Start date for event filtering (YYYY-MM-DD)
- `items_per_page`: (optional) Number of events per page (default: 100)

Example `meltano.yml`:

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
        on24_start_date: "2025-01-01"
        items_per_page: 100
```

---

## Usage

Run the tap to extract events from ON24:

```bash
meltano invoke tap-on24
```

---

## Output

The tap outputs ON24 event records matching the ON24 event API schema.

---

## Roadmap

Support for additional ON24 endpoints and features will be added in future releases.
        client_id: "YOUR_CLIENT_ID"
        access_token_key: "YOUR_ACCESS_TOKEN_KEY"
        access_token_secret: "YOUR_ACCESS_TOKEN_SECRET"
        on24_start_date: "2025-01-01"
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

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
