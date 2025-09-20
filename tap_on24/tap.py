"""ON24 tap class."""

from singer_sdk import Tap
from singer_sdk.typing import PropertiesList, Property, StringType, IntegerType, BooleanType
from tap_on24.streams import ON24EventsStream, ON24AttendeesStream, ON24RegistrantsStream

class TapON24(Tap):
    """Singer tap for ON24 Webinar Platform."""
    name = "tap-on24"

    config_jsonschema = PropertiesList(
        Property("client_id", StringType, required=True),
        Property("access_token_key", StringType, required=True),
        Property("access_token_secret", StringType, required=True),
        Property("on24_start_date", StringType, required=True),
        Property("items_per_page", IntegerType, default=100),
    ).to_dict()

    def discover_streams(self):
        """Return a list of discovered streams."""
        return [ON24EventsStream(self), ON24AttendeesStream(self), ON24RegistrantsStream(self)]