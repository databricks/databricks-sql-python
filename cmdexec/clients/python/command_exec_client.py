import logging
from typing import Dict, Tuple, List

import grpc
from google.protobuf import message

import cmdexec.clients.python.sql_command_service_pb2 as command_pb2
from cmdexec.clients.python.sql_command_service_pb2_grpc import SqlCommandServiceStub
from cmdexec.clients.python.errors import OperationalError, InterfaceError

logger = logging.getLogger(__name__)


def connect(**kwargs):
    return Connection(**kwargs)


class Connection:
    def __init__(self, **kwargs):
        try:
            self.host = kwargs["HOST"]
            self.port = kwargs["PORT"]
        except KeyError:
            raise InterfaceError("Please include arguments HOST and PORT in kwargs for Connection")

        self._base_client = CmdExecBaseHttpClient(self.host, self.port, kwargs.get("metadata", []))
        open_session_request = command_pb2.OpenSessionRequest(
            configuration={},
            client_session_id=None,
            session_info_fields=None,
        )

        try:
            resp = self._base_client.make_request(self._base_client.stub.OpenSession,
                                                  open_session_request)
            self.session_id = resp.id
            logger.info("Successfully opened session " + str(self.session_id.hex()))
        except grpc.RpcError as error:
            raise OperationalError("Error during database connection", error)

    def cursor(self):
        pass

    def close(self):
        try:
            close_session_request = command_pb2.CloseSessionRequest(id=self.session_id)
            self._base_client.make_request(self._base_client.stub.CloseSession,
                                           close_session_request)
        except grpc.RpcError as error:
            raise OperationalError("Error during database connection close", error)


class CmdExecBaseHttpClient:
    """
    A thin wrapper around a gRPC channel that takes cares of headers etc.
    """

    def __init__(self, host: str, port: int, http_headers: List[Tuple[str, str]]):
        self.host_url = host + ":" + str(port)
        self.http_headers = [(k.lower(), v) for (k, v) in http_headers]
        self.channel = grpc.insecure_channel(self.host_url)
        self.stub = SqlCommandServiceStub(self.channel)

    def make_request(self, method, request):
        try:
            response = method(request, metadata=self.http_headers)
            logger.info("Received message: %s", response)
            return response
        except grpc.RpcError as error:
            logger.error("Received error during gRPC request: %s", error)
            raise error
