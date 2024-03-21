"""Request Decorator module."""

import abc

_FORBIDDEN_HEADERS = [
    "SplitSDKVersion",
    "SplitMachineIp",
    "SplitMachineName",
    "SplitImpressionsMode",
    "Host",
    "Referrer",
    "Content-Type",
    "Content-Length",
    "Content-Encoding",
    "Accept",
    "Keep-Alive",
    "X-Fastly-Debug"
]

class UserCustomHeaderDecorator(object, metaclass=abc.ABCMeta):
    """User custom header decorator interface."""

    @abc.abstractmethod
    def get_header_overrides(self):
        """
        Return a dictionary with all the user-defined custom headers.

        :return: Dictionary {String: String}
        :rtype: Dict
        """
        pass

class NoOpHeaderDecorator(UserCustomHeaderDecorator):
    """User custom header Class for no headers."""

    def get_header_overrides(self):
        """
        Return a dictionary with all the user-defined custom headers.

        :return: Dictionary {String: String}
        :rtype: Dict
        """
        return {}

class RequestDecorator(object):
    """Request decorator class for injecting User custom data."""

    def __init__(self, user_custom_header_decorator=None):
        """
        Class constructor.

        :param user_custom_header_decorator: User custom header decorator instance.
        :type user_custom_header_decorator: splitio.api.request_decorator.UserCustomHeaderDecorator
        """
        if user_custom_header_decorator is None:
            user_custom_header_decorator = NoOpHeaderDecorator()

        self._user_custom_header_decorator = user_custom_header_decorator

    def decorate_headers(self, request_session):
        """
        Use a passed header dictionary and append user custom headers from the UserCustomHeaderDecorator instance.

        :param request_session: HTTP Request session
        :type request_session: requests.Session()

        :return: Updated Request session
        :rtype: requests.Session()
        """
        try:
            custom_headers = self._user_custom_header_decorator.get_header_overrides()
            for header in custom_headers:
                if self._is_header_allowed(header):
                    request_session.headers[header] = custom_headers[header]
            return request_session
        except Exception as exc:
            raise ValueError('Problem adding custom header in request decorator') from exc

    def _is_header_allowed(self, header):
        """
        Verivy for a given header if it exists in the list of reserved forbidden headers

        :param header: Dictionary containing header
        :type headers: Dict

        :return: True if does not exist in forbidden headers list, False otherwise
        :rtype: Boolean
        """
        return header not in _FORBIDDEN_HEADERS