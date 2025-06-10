class StringUtil:
    @staticmethod
    def escape_string_literal(s: str) -> str:
        """Escapes single quotes in a string for safe SQL usage."""
        if s is None:
            return None
        return s.replace("'", "''")

    @staticmethod
    def get_volume_path(catalog: str, schema: str, volume: str) -> str:
        """
        Constructs and escapes the volume path in the form of /Volumes/catalog/schema/volume/
        """
        path = f"/Volumes/{catalog}/{schema}/{volume}/"
        return StringUtil.escape_string_literal(path)

    @staticmethod
    def get_object_full_path(
        catalog: str, schema: str, volume: str, object_path: str
    ) -> str:
        """
        Returns the full escaped object path by appending the escaped object name to the volume path.
        """
        return StringUtil.get_volume_path(
            catalog, schema, volume
        ) + StringUtil.escape_string_literal(object_path)

    @staticmethod
    def create_get_object_query(
        catalog: str, schema: str, volume: str, object_path: str, local_path: str
    ) -> str:
        """
        Returns the SQL GET command for retrieving an object to a local path.
        """
        return f"GET '{StringUtil.get_object_full_path(catalog, schema, volume, object_path)}' TO '{StringUtil.escape_string_literal(local_path)}'"

    @staticmethod
    def create_get_object_query_for_input_stream(
        catalog: str, schema: str, volume: str, object_path: str
    ) -> str:
        """
        Constructs a GET query that writes the retrieved object to an input stream placeholder.
        """
        full_path = StringUtil.get_object_full_path(
            catalog, schema, volume, object_path
        )
        return f"GET '{full_path}' TO '__input_stream__'"

    @staticmethod
    def create_put_object_query(
        catalog: str,
        schema: str,
        volume: str,
        object_path: str,
        local_path: str,
        overwrite: bool,
    ) -> str:
        escaped_local_path = StringUtil.escape_string_literal(local_path)
        full_remote_path = StringUtil.get_object_full_path(
            catalog, schema, volume, object_path
        )
        overwrite_clause = " OVERWRITE" if overwrite else ""
        return f"PUT '{escaped_local_path}' INTO '{full_remote_path}'{overwrite_clause}"

    @staticmethod
    def create_put_object_query_for_input_stream(
        catalog: str, schema: str, volume: str, object_path: str, to_overwrite: bool
    ) -> str:
        """
        Constructs a PUT query that uploads from an input stream to a volume path.
        Appends 'OVERWRITE' if to_overwrite is True.
        """
        full_remote_path = StringUtil.get_object_full_path(
            catalog, schema, volume, object_path
        )
        overwrite_clause = " OVERWRITE" if to_overwrite else ""
        return f"PUT '__input_stream__' INTO '{full_remote_path}'{overwrite_clause}"

    @staticmethod
    def get_object_query(
        catalog: str, schema: str, volume: str, object_path: str, local_path: str
    ) -> str:
        """
        Public entry point to create GET object query.
        Equivalent to: String getObjectQuery = createGetObjectQuery(...)
        """
        return StringUtil.create_get_object_query(
            catalog, schema, volume, object_path, local_path
        )

    @staticmethod
    def create_delete_object_query(
        catalog: str, schema: str, volume: str, object_path: str
    ) -> str:
        """
        Returns the SQL REMOVE command for deleting an object from a volume.
        """
        return f"REMOVE '{StringUtil.get_object_full_path(catalog, schema, volume, object_path)}'"
