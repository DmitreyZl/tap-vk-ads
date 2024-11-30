"""VkAds tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from datetime import date, datetime, timedelta

# TODO: Import your custom stream types here:
from tap_vkads import streams


class TapVkAds(Tap):
    """VkAds tap class."""

    name = "tap-vkads"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            title="Auth Token",
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "params",
            th.ObjectType(
                th.Property(
                    "fields",
                    th.StringType,
                    title="Параметры запроса",
                    description="Параметры запроса",
                ),
                th.Property(
                    "metrics",
                    th.StringType,
                    title="Параметры запроса",
                    description="Параметры запроса",
                ),
            ),
        ),
        th.Property(
            "date_from",
            th.DateTimeType,
            title="Начальная дата",
            default=str(date.today() - timedelta(days=1)),
            description="Начальная дата"
        ),
        th.Property(
            "date_to",
            th.StringType,
            title="Конечная дата",
            default=str(date.today()),
            description="Конечная дата"
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.VkAdsStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.CampaignsStream(self),
            streams.CampaignsStatisticStream(self),
        ]


if __name__ == "__main__":
    TapVkAds.cli()
