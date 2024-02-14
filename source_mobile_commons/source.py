#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
# from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_cdk.sources.streams import IncrementalMixin
from datetime import datetime, timedelta, timezone
import json
import sys
import xmltodict

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


# Basic full refresh stream
class MobileCommonsStream(HttpStream, ABC):
    """
    """

    url_base = "https://secure.mcommons.com/api/"

    def __init__(
        self,
        *args,
        username: str = None,
        password: str = None,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._username = username
        self._password = password
        self.object_name = None
        self.array_name = None
        self.force_list = None

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        """
        response_dict = xmltodict.parse(
            xml_input=response.content,
            attr_prefix="",
            cdata_key="",
            process_namespaces=True
        )['response'][self.object_name]

        page = response_dict.get('page')
        num = response_dict.get('num')
        page_count = response_dict.get('page_count')

        # There are two different types of pagination... fun.
        if page and num:
            if int(num) > 0:
                self.page = int(page) + 1
                return {"page": self.page}
            else:
                return None
        elif page and page_count:
            if int(page) < int(page_count):
                self.page = int(page) + 1
                return {"page": self.page}
            else:
                return None
        else:
            return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        """
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        if next_page_token:
            params.update(**next_page_token)
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        """
        response_dict = xmltodict.parse(
            xml_input=response.content,
            attr_prefix="",
            cdata_key="",
            process_namespaces=True,
            force_list=self.force_list
        )['response']
    
        data = response_dict[self.object_name].get(self.array_name)
        # print(json.dumps(data[0]))
        # sys.exit()
        if data:
            yield from data
        else:
            return []

class Broadcasts(MobileCommonsStream):
    """
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.object_name = 'broadcasts'
        self.array_name = 'broadcast'
        self.force_list=['broadcast', 'group', 'tags']
        self.custom_params = {
            "limit": 20
        }

    # TODO: Fill in the cursor_field. Required.
    # cursor_field = "updated_at"

    primary_key = "id"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params.update(self.custom_params)

        return params

    def path(self, **kwargs) -> str:
        """
        """
        return "broadcasts"


class Calls(HttpSubStream, MobileCommonsStream):
    """
    """

    def __init__(self, **kwargs):
        super().__init__(parent=MConnects, **kwargs)
        self.parent = MConnects(**kwargs)
        self.object_name = 'calls'
        self.array_name = 'call'
        self.force_list=[self.array_name]

    def stream_slices(
        self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        for mconnect in self.parent.read_records(sync_mode=SyncMode.full_refresh):
            yield {"mconnect_id": mconnect["id"]}


    primary_key = "id"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params.update(
            {
                "mconnect_id": stream_slice["mconnect_id"],
                "include_profile": True,
                "include_profile_id_only": True,
                "limit": 1000
            }
        )

        return params

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "calls"


class Campaigns(MobileCommonsStream):
    """
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.object_name = 'campaigns'
        self.array_name = 'campaign'
        self.force_list=['campaign', 'tags', 'opt_in_path']
        self.custom_params = {
            "include_opt_in_paths": 1,
        }

    primary_key = "id"

    def use_cache(self) -> bool:
        return True

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params.update(self.custom_params)

        return params

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "campaigns"


class CampaignSubscribers(HttpSubStream, MobileCommonsStream):
    """
    """

    def __init__(self, **kwargs):
        super().__init__(parent=Campaigns, **kwargs)
        self.parent = Campaigns(**kwargs)
        self.object_name = 'subscriptions'
        self.array_name = 'sub'
        self.force_list=['sub']

    def stream_slices(
        self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        for campaign in self.parent.read_records(sync_mode=SyncMode.full_refresh):
            yield {"campaign_id": campaign["id"]}


    primary_key = "id"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params.update({"campaign_id": stream_slice["campaign_id"]})

        return params

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "campaign_subscribers"


class IncomingMessages(MobileCommonsStream):
    """
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.object_name = 'messages'
        self.array_name = 'message'
        self.force_list=['message']
        self.custom_params = {
            "limit": 1000
        }

    primary_key = "id"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params.update(self.custom_params)

        return params

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "messages"


class Keywords(MobileCommonsStream):
    """
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.object_name = 'keywords'
        self.array_name = 'keyword'
        self.force_list = ['keyword']

    # TODO: Fill in the cursor_field. Required.
    # cursor_field = "updated_at"

    primary_key = "id"

    def path(self, **kwargs) -> str:
        """
        """
        return "keywords"

class MConnects(MobileCommonsStream):
    """
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.object_name = 'mconnects'
        self.array_name = 'mconnect'
        self.force_list = [self.array_name, 'tags']

    # TODO: Fill in the cursor_field. Required.
    # cursor_field = "updated_at"

    primary_key = "id"

    def path(self, **kwargs) -> str:
        """
        """
        return "mconnects"


class OutgoingMessages(MobileCommonsStream, IncrementalMixin):
    """
    """

    cursor_field = "sent_at"
    primary_key = "id"

    def __init__(self, start_datetime: datetime, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.object_name = 'messages'
        self.array_name = 'message'
        self.force_list=['message']
        self.start_datetime = start_datetime
        self._cursor_value = None
        self.custom_params = {
            "limit": 1000
        }

    def _chunk_datetime_range(self, start_date: datetime) -> List[Mapping[str, Any]]:
        """
        Returns a list of each day between the start date and now.
        The return value is a list of dicts {'date': date_string}.
        """
        datetimes = []
        while start_date < datetime.utcnow().replace(tzinfo=timezone.utc):
            start_end_datetimes = {
                "start_time": datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S %Z"),
                "end_time": (start_date + timedelta(days=1)).replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S %Z")
            }
            datetimes.append(start_end_datetimes)
            start_date += timedelta(days=1)
        return datetimes[0:100]

    def stream_slices(
        self,
        sync_mode,
        cursor_field: List[str] = None,
        stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        if stream_state and self.cursor_field in stream_state:
            print('Found stream_state. Starting where we left off...')
            start_datetime = datetime.strptime(stream_state[self.cursor_field], '%Y-%m-%d %H:%M:%S %Z').replace(tzinfo=timezone.utc)

        else:
            print('No stream state. Starting from the beginning...') 
            start_datetime = self.start_datetime
        return self._chunk_datetime_range(start_datetime)


    @property
    def availability_strategy(self) -> Optional[AvailabilityStrategy]:
        return None

    @property
    def state(self) -> Mapping[str, Any]:
        print("Getting state...")
        if self._cursor_value:
            print("_cursor_value exists!!")
            return {self.cursor_field: self._cursor_value.strftime('%Y-%m-%d %H:%M:%S %Z')}
        else:
            print("_cursor_value does not exists...")
            return {self.cursor_field: self.start_datetime.strftime('%Y-%m-%d %H:%M:%S %Z')}
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = datetime.strptime(value[self.cursor_field], '%Y-%m-%d %H:%M:%S %Z').replace(tzinfo=timezone.utc)

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params.update(self.custom_params)
        print(stream_slice)
        params.update(
            {
                "start_time": stream_slice["start_time"],
                "end_time": stream_slice["end_time"]
            }
        )
        print(params)
        return params

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            latest_record_date = datetime.strptime(record[self.cursor_field], '%Y-%m-%d %H:%M:%S %Z').replace(tzinfo=timezone.utc)
            # print(self._cursor_value)
            # print(latest_record_date)
            if self._cursor_value:
                self._cursor_value = max(self._cursor_value, latest_record_date)
            else:
                self._cursor_value = latest_record_date
            yield record

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "sent_messages"


class Profiles(MobileCommonsStream, IncrementalMixin):
    """
    """

    cursor_field = "updated_at"
    primary_key = "id"

    def __init__(self, start_datetime: datetime, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.object_name = 'profiles'
        self.array_name = 'profile'
        self.force_list=['profile', 'custom_column', 'integration', 'subscription']
        self.start_datetime = start_datetime
        self._cursor_value = None
        self.custom_params = {
            "include_custom_columns": True,
            "include_subscriptions": True,
            "include_clicks": False,
            "include_members": False,
        }

    def _chunk_datetime_range(self, start_date: datetime) -> List[Mapping[str, Any]]:
        """
        Returns a list of each day between the start date and now.
        The return value is a list of dicts {'date': date_string}.
        """
        datetimes = []
        while start_date < datetime.utcnow().replace(tzinfo=timezone.utc):
            start_end_datetimes = {
                "start_time": datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S %Z"),
                "end_time": (start_date + timedelta(days=1)).replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S %Z")
            }
            datetimes.append(start_end_datetimes)
            start_date += timedelta(days=1)
        return datetimes

    def stream_slices(
        self,
        sync_mode,
        cursor_field: List[str] = None,
        stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        if stream_state and self.cursor_field in stream_state:
            print('Found stream_state. Starting where we left off...')
            start_datetime = datetime.strptime(stream_state[self.cursor_field], '%Y-%m-%d %H:%M:%S %Z').replace(tzinfo=timezone.utc)

        else:
            print('No stream state. Starting from the beginning...') 
            start_datetime = self.start_datetime
        return self._chunk_datetime_range(start_datetime)

    @property
    def availability_strategy(self) -> Optional[AvailabilityStrategy]:
        return None

    @property
    def state(self) -> Mapping[str, Any]:
        print("Getting state...")
        if self._cursor_value:
            print("_cursor_value exists!!")
            return {self.cursor_field: self._cursor_value.strftime('%Y-%m-%d %H:%M:%S %Z')}
        else:
            print("_cursor_value does not exists...")
            return {self.cursor_field: self.start_datetime.strftime('%Y-%m-%d %H:%M:%S %Z')}
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = datetime.strptime(value[self.cursor_field], '%Y-%m-%d %H:%M:%S %Z').replace(tzinfo=timezone.utc)

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            latest_record_date = datetime.strptime(record[self.cursor_field], '%Y-%m-%d %H:%M:%S %Z').replace(tzinfo=timezone.utc)

            if self._cursor_value:
                self._cursor_value = max(self._cursor_value, latest_record_date)
            else:
                self._cursor_value = latest_record_date
            yield record


    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params.update(self.custom_params)
        params.update(
            {
                "from": stream_slice["start_time"],
                "to": stream_slice["end_time"]
            }
        )
        print(params)
        return params

    def path(self, **kwargs) -> str:
        """
        """
        return "profiles"


# Source
class SourceMobileCommons(AbstractSource):

    def get_basic_auth(self, config: Mapping[str, Any]) -> requests.auth.HTTPBasicAuth:
        return requests.auth.HTTPBasicAuth(config["username"], config["password"])

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        Implement a connection check to validate that the user-provided config can be used to connect to the Mobile Commons API

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """

        # We should change this to leverage the actual stream class.
        check_url = "https://secure.mcommons.com/api/campaigns"

        try:
            auth = self.get_basic_auth(config)
            response = requests.get(check_url, auth=auth)
            if response.status_code == 200:
                logger.info(f"Connection to {check_url} successful.")
                return True, None
            else:
                return False, f"Connection to {check_url} failed with status code: {response.status_code}"

        except requests.RequestException as e:
            return False, f"Connection to {check_url} failed with error: {str(e)}"


    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        auth = self.get_basic_auth(config)
        start_datetime = datetime.strptime(config['start_date'], '%Y-%m-%d').replace(tzinfo=timezone.utc)
        return [
            Broadcasts(authenticator=auth),
            Calls(authenticator=auth),
            Campaigns(authenticator=auth),
            CampaignSubscribers(authenticator=auth),
            IncomingMessages(authenticator=auth),
            Keywords(authenticator=auth),
            MConnects(authenticator=auth),
            OutgoingMessages(authenticator=auth, start_datetime=start_datetime),
            Profiles(authenticator=auth, start_datetime=start_datetime),
        ]
