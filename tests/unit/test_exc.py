import databricks.sql.exc as exc


class TestError:
    def test_str_returns_string_when_message_is_none(self):
        """object.__str__ must return a str; a None message must not raise
        TypeError: __str__ returned non-string (type NoneType)."""
        e = exc.Error()
        result = str(e)
        assert isinstance(result, str)

    def test_message_with_context_when_message_is_none(self):
        """message_with_context must not raise on `None + str` when message
        is None; it should still serialize the context."""
        e = exc.Error(context={"foo": "bar"})
        result = e.message_with_context()
        assert isinstance(result, str)
        assert "foo" in result
        assert "bar" in result
