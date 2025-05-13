from typing import BinaryIO

from databricks.sql.utils.string_util import StringUtil


class VolumeClient:
    """
    Databricks Volume Client
    """

    def __init__(self, conn):
        """
        Initialize the VolumeClient with a connection object.

        :param conn: Connection object to Databricks.
        """
        self.conn = conn

    def get_object(
        self, catalog: str, schema: str, volume: str, object_path: str, local_path: str
    ) -> bool:
        get_object_query = StringUtil.create_get_object_query(
            catalog, schema, volume, object_path, local_path
        )
        return True

    def get_object(
        self, catalog: str, schema: str, volume: str, object_path: str
    ) -> BinaryIO:
        get_object_query = StringUtil.create_get_object_query_for_input_stream(
            catalog, schema, volume, object_path
        )
        return True

    def put_object(
        self,
        catalog: str,
        schema: str,
        volume: str,
        object_path: str,
        local_path: str,
        to_overwrite: bool,
    ) -> bool:
        put_object_query = StringUtil.create_put_object_query(
            catalog, schema, volume, object_path, local_path, to_overwrite
        )
        return True

    def put_object(
        self,
        catalog: str,
        schema: str,
        volume: str,
        object_path: str,
        input_stream: BinaryIO,
        content_length: int,
        to_overwrite: bool,
    ) -> bool:
        put_object_query_for_input_stream = (
            StringUtil.create_put_object_query_for_input_stream(
                catalog, schema, volume, object_path, to_overwrite
            )
        )
        return True

    def delete_object(
        self, catalog: str, schema: str, volume: str, object_path: str
    ) -> bool:
        delete_object_query = StringUtil.create_delete_object_query(
            catalog, schema, volume, object_path
        )
        return True
