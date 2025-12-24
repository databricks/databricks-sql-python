import os
import tempfile

import pytest
import databricks.sql as sql
from databricks.sql import Error


@pytest.fixture(scope="module", autouse=True)
def check_catalog_and_schema(catalog, schema):
    """This fixture verifies that a catalog and schema are present in the environment.
    The fixture only evaluates when the test _isn't skipped_.
    """

    if catalog is None or schema is None:
        raise ValueError(
            f"UC Volume tests require values for the `catalog` and `schema` environment variables. Found catalog {_catalog} schema {_schema}"
        )


class PySQLUCVolumeTestSuiteMixin:
    """Simple namespace for UC Volume tests.

    In addition to connection credentials (host, path, token) this suite requires env vars
    named catalog and schema"""

    def test_uc_volume_life_cycle(self, catalog, schema):
        """PUT a file into the UC Volume
        GET the file from the UC Volume
        REMOVE the file from the UC Volume
        Try to GET the file again expecting to raise an exception
        """

        # PUT should succeed

        fh, temp_path = tempfile.mkstemp()

        original_text = "hello world!".encode("utf-8")

        with open(fh, "wb") as fp:
            fp.write(original_text)

        with self.connection(
            extra_params={"staging_allowed_local_path": temp_path}
        ) as conn:

            cursor = conn.cursor()
            query = f"PUT '{temp_path}' INTO '/Volumes/{catalog}/{schema}/e2etests/file1.csv' OVERWRITE"
            cursor.execute(query)

        # GET should succeed

        new_fh, new_temp_path = tempfile.mkstemp()

        with self.connection(
            extra_params={"staging_allowed_local_path": new_temp_path}
        ) as conn:
            cursor = conn.cursor()
            query = f"GET '/Volumes/{catalog}/{schema}/e2etests/file1.csv' TO '{new_temp_path}'"
            cursor.execute(query)

        with open(new_fh, "rb") as fp:
            fetched_text = fp.read()

        assert fetched_text == original_text

        # REMOVE should succeed

        remove_query = f"REMOVE '/Volumes/{catalog}/{schema}/e2etests/file1.csv'"

        # Use minimal retry settings to fail fast
        extra_params = {
            "staging_allowed_local_path": "/",
            "_retry_stop_after_attempts_count": 1,
            "_retry_delay_max": 10,
        }
        with self.connection(extra_params=extra_params) as conn:
            cursor = conn.cursor()
            cursor.execute(remove_query)

            # GET after REMOVE should fail

            with pytest.raises(
                Error, match="Staging operation over HTTP was unsuccessful: 404"
            ):
                cursor = conn.cursor()
                query = f"GET '/Volumes/{catalog}/{schema}/e2etests/file1.csv' TO '{new_temp_path}'"
                cursor.execute(query)

        os.remove(temp_path)
        os.remove(new_temp_path)

    def test_uc_volume_put_fails_without_staging_allowed_local_path(
        self, catalog, schema
    ):
        """PUT operations are not supported unless the connection was built with
        a parameter called staging_allowed_local_path
        """

        fh, temp_path = tempfile.mkstemp()

        original_text = "hello world!".encode("utf-8")

        with open(fh, "wb") as fp:
            fp.write(original_text)

        with pytest.raises(
            Error, match="You must provide at least one staging_allowed_local_path"
        ):
            with self.connection() as conn:
                cursor = conn.cursor()
                query = f"PUT '{temp_path}' INTO '/Volumes/{catalog}/{schema}/e2etests/file1.csv' OVERWRITE"
                cursor.execute(query)

    def test_uc_volume_put_fails_if_localFile_not_in_staging_allowed_local_path(
        self, catalog, schema
    ):

        fh, temp_path = tempfile.mkstemp()

        original_text = "hello world!".encode("utf-8")

        with open(fh, "wb") as fp:
            fp.write(original_text)

        base_path, filename = os.path.split(temp_path)

        # Add junk to base_path
        base_path = os.path.join(base_path, "temp")

        with pytest.raises(
            Error,
            match="Local file operations are restricted to paths within the configured staging_allowed_local_path",
        ):
            with self.connection(
                extra_params={"staging_allowed_local_path": base_path}
            ) as conn:
                cursor = conn.cursor()
                query = f"PUT '{temp_path}' INTO '/Volumes/{catalog}/{schema}/e2etests/file1.csv' OVERWRITE"
                cursor.execute(query)

    def test_uc_volume_put_fails_if_file_exists_and_overwrite_not_set(
        self, catalog, schema
    ):
        """PUT a file into the staging location twice. First command should succeed. Second should fail."""

        fh, temp_path = tempfile.mkstemp()

        original_text = "hello world!".encode("utf-8")

        with open(fh, "wb") as fp:
            fp.write(original_text)

        def perform_put():
            with self.connection(
                extra_params={"staging_allowed_local_path": temp_path}
            ) as conn:
                cursor = conn.cursor()
                query = f"PUT '{temp_path}' INTO '/Volumes/{catalog}/{schema}/e2etests/file1.csv'"
                cursor.execute(query)

        def perform_remove():
            try:
                remove_query = (
                    f"REMOVE '/Volumes/{catalog}/{schema}/e2etests/file1.csv'"
                )

                with self.connection(
                    extra_params={"staging_allowed_local_path": "/"}
                ) as conn:
                    cursor = conn.cursor()
                    cursor.execute(remove_query)
            except Exception:
                pass

        # Make sure file does not exist
        perform_remove()

        # Put the file
        perform_put()

        # Try to put it again
        with pytest.raises(
            sql.exc.ServerOperationError, match="FILE_IN_STAGING_PATH_ALREADY_EXISTS"
        ):
            perform_put()

        # Clean up after ourselves
        perform_remove()

    def test_uc_volume_put_fails_if_absolute_localFile_not_in_staging_allowed_local_path(
        self, catalog, schema
    ):
        """
        This test confirms that staging_allowed_local_path and target_file are resolved into absolute paths.
        """

        # If these two paths are not resolved absolutely, they appear to share a common path of /var/www/html
        # after resolution their common path is only /var/www which should raise an exception
        # Because the common path must always be equal to staging_allowed_local_path
        staging_allowed_local_path = "/var/www/html"
        target_file = "/var/www/html/../html1/not_allowed.html"

        with pytest.raises(
            Error,
            match="Local file operations are restricted to paths within the configured staging_allowed_local_path",
        ):
            with self.connection(
                extra_params={"staging_allowed_local_path": staging_allowed_local_path}
            ) as conn:
                cursor = conn.cursor()
                query = f"PUT '{target_file}' INTO '/Volumes/{catalog}/{schema}/e2etests/file1.csv' OVERWRITE"
                cursor.execute(query)

    def test_uc_volume_empty_local_path_fails_to_parse_at_server(self, catalog, schema):
        staging_allowed_local_path = "/var/www/html"
        target_file = ""

        with pytest.raises(Error, match="EMPTY_LOCAL_FILE_IN_STAGING_ACCESS_QUERY"):
            with self.connection(
                extra_params={"staging_allowed_local_path": staging_allowed_local_path}
            ) as conn:
                cursor = conn.cursor()
                query = f"PUT '{target_file}' INTO '/Volumes/{catalog}/{schema}/e2etests/file1.csv' OVERWRITE"
                cursor.execute(query)

    def test_uc_volume_invalid_volume_path_fails_at_server(self, catalog, schema):
        staging_allowed_local_path = "/var/www/html"
        target_file = "index.html"

        with pytest.raises(Error, match="NOT_FOUND: Catalog"):
            with self.connection(
                extra_params={"staging_allowed_local_path": staging_allowed_local_path}
            ) as conn:
                cursor = conn.cursor()
                query = f"PUT '{target_file}' INTO '/Volumes/RANDOMSTRINGOFCHARACTERS/{catalog}/{schema}/e2etests/file1.csv' OVERWRITE"
                cursor.execute(query)

    def test_uc_volume_supports_multiple_staging_allowed_local_path_values(
        self, catalog, schema
    ):
        """staging_allowed_local_path may be either a path-like object or a list of path-like objects.

        This test confirms that two configured base paths:
        1 - doesn't raise an exception
        2 - allows uploads from both paths
        3 - doesn't allow uploads from a third path
        """

        def generate_file_and_path_and_queries():
            """
            1. Makes a temp file with some contents.
            2. Write a query to PUT it into a staging location
            3. Write a query to REMOVE it from that location (for cleanup)
            """
            fh, temp_path = tempfile.mkstemp()
            with open(fh, "wb") as fp:
                original_text = "hello world!".encode("utf-8")
                fp.write(original_text)
            put_query = f"PUT '{temp_path}' INTO '/Volumes/{catalog}/{schema}/e2etests/{id(temp_path)}.csv' OVERWRITE"
            remove_query = (
                f"REMOVE '/Volumes/{catalog}/{schema}/e2etests/{id(temp_path)}.csv'"
            )
            return fh, temp_path, put_query, remove_query

        (
            fh1,
            temp_path1,
            put_query1,
            remove_query1,
        ) = generate_file_and_path_and_queries()
        (
            fh2,
            temp_path2,
            put_query2,
            remove_query2,
        ) = generate_file_and_path_and_queries()
        (
            fh3,
            temp_path3,
            put_query3,
            remove_query3,
        ) = generate_file_and_path_and_queries()

        with self.connection(
            extra_params={"staging_allowed_local_path": [temp_path1, temp_path2]}
        ) as conn:
            cursor = conn.cursor()

            cursor.execute(put_query1)
            cursor.execute(put_query2)

            with pytest.raises(
                Error,
                match="Local file operations are restricted to paths within the configured staging_allowed_local_path",
            ):
                cursor.execute(put_query3)

            # Then clean up the files we made
            cursor.execute(remove_query1)
            cursor.execute(remove_query2)
