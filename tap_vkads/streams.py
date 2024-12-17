"""Stream type classes for tap-vkads."""

from __future__ import annotations
import requests
import typing as t
from importlib import resources
import json
from datetime import date, datetime, timedelta
from singer_sdk.helpers.jsonpath import extract_jsonpath

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_vkads.client import VkAdsStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class CampaignsStream(VkAdsStream):
    """Define custom stream."""

    name = "campaigns"
    path = "/campaigns.json"
    primary_keys = ["id"]
    replication_key = "id"
    cont = {}
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"  # noqa: ERA001
    #id,name,created,date_start,date_end,utm,objective,budget_limit,budget_limit_day
    schema = th.PropertiesList(
        th.Property(
            "id",
            th.StringType,
            description="The campaign's system ID",
        ),
        th.Property("name", th.StringType),
        th.Property("account", th.StringType),
        th.Property("status", th.StringType),
        th.Property("ad_plan_id", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("date_start", th.DateType),
        th.Property("date_end", th.DateType),
        th.Property("utm", th.StringType),
        th.Property("objective", th.StringType),
        th.Property("budget_limit", th.StringType),
        th.Property("budget_limit_day", th.StringType),
    ).to_dict()

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.
            context
        Yields:
            Each record from the source.
        """
        res = response.json().get('items')
        for record in res:
            record.update({'account': self.config.get("account")})
        #    self.logger.error(json.dumps(record))
        self.cont["ids"] = [record.get('id') for record in res]  # Обновляем состояние
        # Логируем изменения в контексте

        self.logger.info(f"Updated context: {self.cont}")
        yield from extract_jsonpath(self.records_jsonpath, input=res)


class AdGroupStream(VkAdsStream):
    """Define custom stream."""

    name = "ad_plans"
    path = "/ad_plans.json"
    primary_keys = ["id"]
    replication_key = "id"
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"  # noqa: ERA001
    #id,name,created,date_start,date_end,utm,objective,budget_limit,budget_limit_day
    schema = th.PropertiesList(
        th.Property(
            "id",
            th.StringType,
            description="The adsGroup's system ID",
        ),
        th.Property("name", th.StringType),
        th.Property("account", th.StringType),
        th.Property("status", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("date_start", th.DateType),
        th.Property("date_end", th.DateType),
        th.Property("objective", th.StringType),
        th.Property("budget_limit", th.StringType),
        th.Property("budget_limit_day", th.StringType)
    ).to_dict()

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
        params = {'fields': 'id,name,status,created,date_start,date_end,objective,budget_limit,budget_limit_day'}
        params["limit"] = 250
        if next_page_token:
            params["offset"] = next_page_token
        self.logger.error(params)
        return params

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.
            context
        Yields:
            Each record from the source.
        """
        self.logger.error(response.text)
        res = response.json().get('items')
        for record in res:
            record.update({'account': self.config.get("account")})
        #    self.logger.error(json.dumps(record))
        #self.cont["ids"] = [record.get('id') for record in res]  # Обновляем состояние
        # Логируем изменения в контексте

        #self.logger.info(f"Updated context: {self.cont}")
        yield from extract_jsonpath(self.records_jsonpath, input=res)


class CampaignsStatisticStream(CampaignsStream):
    """Define custom stream."""

    name = "statistics_campaigns"
    path = "/statistics/campaigns/day.json"
    primary_keys = ["id", "date"]
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("id", th.IntegerType),
        #base
        th.Property("base.shows", th.StringType),
        th.Property("base.clicks", th.StringType),
        th.Property("base.goals", th.StringType),
        th.Property("base.spent", th.StringType),
        th.Property("base.cpm", th.StringType),
        th.Property("base.cpc", th.StringType),
        th.Property("base.cpa", th.StringType),
        th.Property("base.ctr", th.StringType),
        th.Property("base.cr", th.StringType),
        th.Property("base.vk.goals", th.StringType),
        th.Property("base.vk.cpa", th.StringType),
        th.Property("base.vk.cr", th.StringType),
        #events
        th.Property("events.opening_app", th.StringType),
        th.Property("events.opening_post", th.StringType),
        th.Property("events.moving_into_group", th.StringType),
        th.Property("events.clicks_on_external_url", th.StringType),
        th.Property("events.launching_video", th.StringType),
        th.Property("events.comments", th.StringType),
        th.Property("events.joinings", th.StringType),
        th.Property("events.likes", th.StringType),
        th.Property("events.shares", th.StringType),
        th.Property("events.votings", th.StringType),
        th.Property("events.sending_form", th.StringType),
        # uniques
        th.Property("uniques.initial_total", th.StringType),
        th.Property("uniques.total", th.StringType),
        th.Property("uniques.increment", th.StringType),
        th.Property("uniques.frequency", th.StringType),
        # video
        th.Property("video.started", th.StringType),
        th.Property("video.paused", th.StringType),
        th.Property("video.resumed_after_pause", th.StringType),
        th.Property("video.fullscreen_on", th.StringType),
        th.Property("video.fullscreen_off", th.StringType),
        th.Property("video.sound_turned_off", th.StringType),
        th.Property("video.sound_turned_on", th.StringType),
        th.Property("video.viewed_10_seconds", th.StringType),
        th.Property("video.viewed_25_percent", th.StringType),
        th.Property("video.viewed_50_percent", th.StringType),
        th.Property("video.viewed_75_percent", th.StringType),
        th.Property("video.viewed_100_percent", th.StringType),
        th.Property("video.viewed_10_seconds_rate", th.StringType),
        th.Property("video.viewed_25_percent_rate", th.StringType),
        th.Property("video.viewed_50_percent_rate", th.StringType),
        th.Property("video.viewed_75_percent_rate", th.StringType),
        th.Property("video.viewed_100_percent_rate", th.StringType),
        th.Property("video.depth_of_view", th.StringType),
        th.Property("video.viewed_10_seconds_cost", th.StringType),
        th.Property("video.viewed_25_percent_cost", th.StringType),
        th.Property("video.viewed_50_percent_cost", th.StringType),
        th.Property("video.viewed_75_percent_cost", th.StringType),
        th.Property("video.viewed_100_percent_cost", th.StringType),
        th.Property("video.viewed_range_rate", th.StringType),
        th.Property("video.started_cost", th.StringType),
        #romi
        th.Property("romi.value", th.StringType),
        th.Property("romi.romi", th.StringType),
        th.Property("romi.adv_cost_share", th.StringType),
        #ad_offers
        th.Property("ad_offers.offer_postponed", th.StringType),
        th.Property("ad_offers.upload_receipt", th.StringType),
        th.Property("ad_offers.earn_offer_rewards", th.StringType),
        # social_network
        th.Property("social_network.vk_join", th.StringType),
        th.Property("social_network.vk_subscribe", th.StringType),
        th.Property("social_network.ok_join", th.StringType),
        th.Property("social_network.dzen_join", th.StringType),
        th.Property("social_network.result_join", th.StringType),
        th.Property("social_network.vk_message", th.StringType),
        th.Property("social_network.ok_message", th.StringType),
        th.Property("social_network.result_message", th.StringType),
    ).to_dict()

    def get_url_params(
            self,
            context,  # noqa: ARG002
            next_page_token,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        ids = self.cont.get("ids", [])
        params = self.config.get("params")
        params['id'] = ','.join(map(str, ids))
        params['id'] = params['id'].replace(' ', '')
        self.logger.error(json.dumps(params))
        if self.config.get("date_from"):
            params["date_from"] = self.config.get("date_from")
        if self.config.get("date_to"):
            params["date_to"] = self.config.get("date_to")
        params["limit"] = 250
        if next_page_token:
            params["offset"] = next_page_token
        return params

    def flatten_dict(self, d, parent_key='', sep='.'):
        """
        Разворачивает вложенные словари в плоский словарь.

        :param d: Исходный словарь
        :param parent_key: Префикс для ключей
        :param sep: Разделитель между уровнями вложенности
        :return: Плоский словарь
        """
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):  # Если значение — словарь, обрабатываем рекурсивно
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        self.logger.error(response.text)
        result = []
        for i in response.json().get('items'):
            if 'rows' in i:
                res = i.get('rows')
                for r in res:
                    r.update({'id': i.get('id')})
                    r.update(self.flatten_dict(r))
                    result.append(r)
        yield from extract_jsonpath(self.records_jsonpath, input=result)
