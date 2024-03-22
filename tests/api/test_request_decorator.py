"""Request Decorator test module."""
import requests
import pytest

from splitio.api.request_decorator import RequestDecorator, UserCustomHeaderDecorator, _FORBIDDEN_HEADERS

class RequestDecoratorTests(object):
    """Request Decorator test cases."""

    def test_noop(self):
        """Test no operation."""
        decorator = RequestDecorator()
        session = requests.Session()
        old_len = len(session.headers)
        session = decorator.decorate_headers(session)
        assert(len(session.headers) == old_len)

    def test_add_custom_headers(self):
        """test adding custom headers."""

        class MyCustomDecorator(UserCustomHeaderDecorator):
            def get_header_overrides(self):
                return {"UserCustomHeader": "value", "AnotherCustomHeader": "val"}

        decorator = RequestDecorator(MyCustomDecorator())
        session = requests.Session()
        session = decorator.decorate_headers(session)
        assert(session.headers["UserCustomHeader"] == "value")
        assert(session.headers["AnotherCustomHeader"] == "val")

    def test_add_forbidden_headers(self):
        """test adding forbidden headers."""

        class MyCustomDecorator(UserCustomHeaderDecorator):
            def get_header_overrides(self):
                final_header = {"UserCustomHeader": "value"}
                [final_header.update({header: "val"}) for header in _FORBIDDEN_HEADERS]
                [final_header.update({header.lower(): "val"}) for header in _FORBIDDEN_HEADERS]
                return final_header

        decorator = RequestDecorator(MyCustomDecorator())
        session = requests.Session()
        session = decorator.decorate_headers(session)
        assert(session.headers["UserCustomHeader"] == "value")

    def test_errors(self):
        class MyCustomDecorator(UserCustomHeaderDecorator):
            def get_header_overrides(self):
                return ["MyCustomHeader"]

        decorator = RequestDecorator(MyCustomDecorator())
        session = requests.Session()
        with pytest.raises(ValueError):
            session = decorator.decorate_headers(session)
