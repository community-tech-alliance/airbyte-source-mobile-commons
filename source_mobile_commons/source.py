#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
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
        # print(json.dumps(data))
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


class Clicks(MobileCommonsStream):
    """
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.object_name = 'clicks'
        self.array_name = 'click'
        self.force_list = [self.array_name]
        self.custom_params = {
            "include_profile": False,
            "total_link_clicks": False
        }

    def stream_slices(
        self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        for tiny_url in self.parent.read_records(sync_mode=SyncMode.full_refresh):
            yield {"url_id": tiny_url["id"]}

    primary_key = "id"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params.update({"url_id": stream_slice["url_id"]})

        return params

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "clicks"


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


# Basic incremental stream
class IncrementalMobileCommonsStream(MobileCommonsStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}

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


class OutgoingMessages(MobileCommonsStream):
    """
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.object_name = 'messages'
        self.array_name = 'message'
        self.force_list=['message']
        self.custom_params = {
            "limit": 1000,
            "start_time": "2024-01-01" # Adding temporarily
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

        return "sent_messages"


# class Profiles(IncrementalMobileCommonsStream):
class Profiles(MobileCommonsStream):
    """
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.object_name = 'profiles'
        self.array_name = 'profile'
        self.force_list=['profile', 'custom_column', 'integration', 'subscription']
        self.custom_params = {
            "include_custom_columns": True,
            "include_subscriptions": True,
            "include_clicks": False,
            "include_members": False,
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
        return "profiles"

    # def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
    #     """
    #     TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

    #     Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
    #     This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
    #     section of the docs for more information.

    #     The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
    #     necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
    #     This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

    #     An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
    #     craft that specific request.

    #     For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
    #     this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
    #     till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
    #     the date query param.
    #     """
    #     raise NotImplementedError("Implement stream slices or delete this method!")


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
        return [
            Broadcasts(authenticator=auth),
            Calls(authenticator=auth),
            Campaigns(authenticator=auth),
            CampaignSubscribers(authenticator=auth),
            Clicks(authenticator=auth),
            IncomingMessages(authenticator=auth),
            Keywords(authenticator=auth),
            MConnects(authenticator=auth),
            OutgoingMessages(authenticator=auth),
            Profiles(authenticator=auth),
            TinyUrls(authenticator=auth),
        ]


class TinyUrls(MobileCommonsStream):
    """
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.object_name = 'tinyurls'
        self.array_name = 'tinyurl'
        self.force_list = [self.array_name]

    # TODO: Fill in the cursor_field. Required.
    # cursor_field = "updated_at"

    primary_key = "id"

    def path(self, **kwargs) -> str:
        """
        """
        return "tinyurls"