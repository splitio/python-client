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

class RequestContext(object):
    """Request conext class."""

    def __init__(self, headers):
        """
        Class constructor.

        :param headers: Custom headers dictionary
        :type headers: Dict
        """
        self._headers = headers

    def headers(self):
        """
        Return a dictionary with all the user-defined custom headers.

        :return: Dictionary {String: [String]}
        :rtype: Dict
        """
        return self._headers

class CustomHeaderDecorator(object, metaclass=abc.ABCMeta):
    """User custom header decorator interface."""

    @abc.abstractmethod
    def get_header_overrides(self):
        """
        Return a dictionary with all the user-defined custom headers.

        :return: Dictionary {String: String}
        :rtype: Dict
        """
        pass

class NoOpHeaderDecorator(CustomHeaderDecorator):
    """User custom header Class for no headers."""

    def get_header_overrides(self, request_context):
        """
        Return a dictionary with all the user-defined custom headers.

        :param request_context: Request context instance
        :type request_context: splitio.api.request_decorator.RequestContext

        :return: Dictionary {String: [String]}
        :rtype: Dict
        """
        return {}

class RequestDecorator(object):
    """Request decorator class for injecting User custom data."""

    def __init__(self, custom_header_decorator=None):
        """
        Class constructor.

        :param custom_header_decorator: User custom header decorator instance.
        :type custom_header_decorator: splitio.api.request_decorator.CustomHeaderDecorator
        """
        if custom_header_decorator is None:
            custom_header_decorator = NoOpHeaderDecorator()

        self._custom_header_decorator = custom_header_decorator

    def decorate_headers(self, new_headers):
        """
        Use a passed header dictionary and append user custom headers from the UserCustomHeaderDecorator instance.

        :param new_headers: Dict of headers
        :type new_headers: Dict

        :return: Updated headers
        :rtype: Dict
        """
        custom_headers = self._custom_header_decorator.get_header_overrides(RequestContext(new_headers))
        try:
            for header in custom_headers:
                if self._is_header_allowed(header):
                    if isinstance(custom_headers[header], list):
                        new_headers[header] = ','.join(custom_headers[header])
                    else:
                        new_headers[header] = custom_headers[header]

            return new_headers
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
        return header.lower() not in [forbidden.lower() for forbidden in _FORBIDDEN_HEADERS]
