import inspect
import pytest

from databricks.sql.thrift_api.TCLIService import ttypes


class TestThriftFieldIds:
    """
    Unit test to validate that all Thrift-generated field IDs comply with the maximum limit.

    Field IDs in Thrift must stay below 3329 to avoid conflicts with reserved ranges
    and ensure compatibility with various Thrift implementations and protocols.
    """

    MAX_ALLOWED_FIELD_ID = 3329

    # Known exceptions that exceed the field ID limit
    KNOWN_EXCEPTIONS = {
        ("TExecuteStatementReq", "enforceEmbeddedSchemaCorrectness"): 3353,
        ("TSessionHandle", "serverProtocolVersion"): 3329,
    }

    def test_all_thrift_field_ids_are_within_allowed_range(self):
        """
        Validates that all field IDs in Thrift-generated classes are within the allowed range.

        This test prevents field ID conflicts and ensures compatibility with different
        Thrift implementations and protocols.
        """
        violations = []

        # Get all classes from the ttypes module
        for name, obj in inspect.getmembers(ttypes):
            if (
                inspect.isclass(obj)
                and hasattr(obj, "thrift_spec")
                and obj.thrift_spec is not None
            ):

                self._check_class_field_ids(obj, name, violations)

        if violations:
            error_message = self._build_error_message(violations)
            pytest.fail(error_message)

    def _check_class_field_ids(self, cls, class_name, violations):
        """
        Checks all field IDs in a Thrift class and reports violations.

        Args:
            cls: The Thrift class to check
            class_name: Name of the class for error reporting
            violations: List to append violation messages to
        """
        thrift_spec = cls.thrift_spec

        if not isinstance(thrift_spec, (tuple, list)):
            return

        for spec_entry in thrift_spec:
            if spec_entry is None:
                continue

            # Thrift spec format: (field_id, field_type, field_name, ...)
            if isinstance(spec_entry, (tuple, list)) and len(spec_entry) >= 3:
                field_id = spec_entry[0]
                field_name = spec_entry[2]

                # Skip known exceptions
                if (class_name, field_name) in self.KNOWN_EXCEPTIONS:
                    continue

                if isinstance(field_id, int) and field_id >= self.MAX_ALLOWED_FIELD_ID:
                    violations.append(
                        "{} field '{}' has field ID {} (exceeds maximum of {})".format(
                            class_name,
                            field_name,
                            field_id,
                            self.MAX_ALLOWED_FIELD_ID - 1,
                        )
                    )

    def _build_error_message(self, violations):
        """
        Builds a comprehensive error message for field ID violations.

        Args:
            violations: List of violation messages

        Returns:
            Formatted error message
        """
        error_message = (
            "Found Thrift field IDs that exceed the maximum allowed value of {}.\n"
            "This can cause compatibility issues and conflicts with reserved ID ranges.\n"
            "Violations found:\n".format(self.MAX_ALLOWED_FIELD_ID - 1)
        )

        for violation in violations:
            error_message += "  - {}\n".format(violation)

        return error_message
