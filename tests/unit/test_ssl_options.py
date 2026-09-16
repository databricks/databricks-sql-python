from unittest.mock import Mock

import pytest

from databricks.sql.exc import ProgrammingError
from databricks.sql.types import SSLOptions


class TestSSLOptionsMutualTls:
    def test_private_key_without_client_certificate_is_rejected_before_file_access(
        self,
    ):
        options = SSLOptions(
            tls_client_cert_key_file="/path/does/not/need/to/exist.pem"
        )

        with pytest.raises(ProgrammingError) as exc_info:
            options.load_client_cert_chain(Mock())

        message = str(exc_info.value)
        assert "tls_client_cert_key_file" in message
        assert "tls_client_cert_file" in message
        assert "requires" in message

    @pytest.mark.parametrize(
        "failing_input,empty",
        [
            ("certificate", False),
            ("private key", False),
            ("certificate", True),
            ("private key", True),
        ],
        ids=[
            "missing-certificate",
            "missing-private-key",
            "empty-certificate",
            "empty-private-key",
        ],
    )
    def test_unreadable_or_empty_identity_file_names_failing_input(
        self, tmp_path, failing_input, empty
    ):
        # Deliberately not PEM: file readability/emptiness must be checked for both
        # inputs before SSL parsing begins, so a malformed peer cannot mask the
        # missing/empty input this case is exercising.
        readable_nonempty = tmp_path / "readable-nonempty.pem"
        readable_nonempty.write_bytes(b"not PEM, but readable and non-empty")
        failing_path = tmp_path / ("empty.pem" if empty else "missing.pem")
        if empty:
            failing_path.write_bytes(b"")

        if failing_input == "certificate":
            cert_file = failing_path
            key_file = readable_nonempty
            expected_option = "tls_client_cert_file"
        else:
            cert_file = readable_nonempty
            key_file = failing_path
            expected_option = "tls_client_cert_key_file"

        ssl_context = Mock()
        options = SSLOptions(
            tls_client_cert_file=str(cert_file),
            tls_client_cert_key_file=str(key_file),
        )

        with pytest.raises(ProgrammingError) as exc_info:
            options.load_client_cert_chain(ssl_context)

        message = str(exc_info.value)
        assert expected_option in message
        assert str(failing_path) in message
        assert ("is empty" in message) is empty
        ssl_context.load_cert_chain.assert_not_called()

    def test_combined_cert_key_file_and_password_are_forwarded(self, tmp_path):
        combined = tmp_path / "combined.pem"
        combined.write_bytes(b"non-empty combined PEM placeholder")
        ssl_context = Mock()
        password = "encrypted-key-password"

        SSLOptions(
            tls_client_cert_file=str(combined),
            tls_client_cert_key_password=password,
        ).load_client_cert_chain(ssl_context)

        ssl_context.load_cert_chain.assert_called_once_with(
            certfile=str(combined), keyfile=None, password=password
        )
