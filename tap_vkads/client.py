"""REST client handling, including VkAdsStream base class."""

from __future__ import annotations

import typing as t
from importlib import resources
import json
import sys

from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream


from singer_sdk.pagination import BaseOffsetPaginator

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"


class VkAdsStream(RESTStream):
    """VkAds stream class."""

    # Update this value if necessary or override `parse_response`.
    records_jsonpath = "$[*]"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        # TODO: hardcode a value here, or retrieve it from self.config
        return "https://ads.vk.com/api/v2"

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BearerTokenAuthenticator.create_for_stream(
            self,
            token=self.config.get("auth_token", ""),
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")  # noqa: ERA001
        return {}

    def get_url_params(
            self,
            context: Context | None,  # noqa: ARG002
            next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params = self.config.get("params") or {}
        params["limit"] = 250
        if next_page_token:
            params["offset"] = next_page_token
        return params

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.
            context
        Yields:
            Each record from the source.
        """
        res = response.json().get('items')
        #for record in res:
        #    self.logger.error(json.dumps(record))

        yield from extract_jsonpath(self.records_jsonpath, input=res)
