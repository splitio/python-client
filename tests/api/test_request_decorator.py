"""Request Decorator test module."""
import requests
import pytest

from splitio.api.request_decorator import RequestDecorator, CustomHeaderDecorator, _FORBIDDEN_HEADERS, RequestContext

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

        class MyCustomDecorator(CustomHeaderDecorator):
            def get_header_overrides(self, request_context):
                headers = request_context.headers()
                headers["UserCustomHeader"] = ["value"]
                headers["AnotherCustomHeader"] = ["val1", "val2"]
                return headers

        decorator = RequestDecorator(MyCustomDecorator())
        session = requests.Session()
        session = decorator.decorate_headers(session)
        assert(session.headers["UserCustomHeader"] == ["value"])
        assert(session.headers["AnotherCustomHeader"] == ["val1", "val2"])

    def test_add_forbidden_headers(self):
        """test adding forbidden headers."""

        class MyCustomDecorator(CustomHeaderDecorator):
            def get_header_overrides(self, request_context):
                headers = request_context.headers()
                headers["UserCustomHeader"] = ["value"]
                for header in _FORBIDDEN_HEADERS:
                    headers[header] = ["val"]
                return headers

        decorator = RequestDecorator(MyCustomDecorator())
        session = requests.Session()
        session = decorator.decorate_headers(session)
        assert(session.headers["UserCustomHeader"] == ["value"])

    def test_errors(self):
        class MyCustomDecorator(CustomHeaderDecorator):
            def get_header_overrides(self, request_context):
                return ["MyCustomHeader"]

        decorator = RequestDecorator(MyCustomDecorator())
        session = requests.Session()
        with pytest.raises(ValueError):
            session = decorator.decorate_headers(session)
