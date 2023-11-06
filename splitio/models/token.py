"""Token module"""

import base64
import json


class Token(object):
    """Token object class."""

    def __init__(self, push_enabled, token, channels, exp, iat):
        """
        Class constructor.

        :param push_enabled: flag push enabled.
        :type push_enabled: bool

        :param token: Token from auth.
        :type token: str

        :param channels: Channels parsed from token.
        :type channels: str

        :param exp: exp parsed from token.
        :type exp: int

        :param iat: iat parsed from token.
        :type iat: int
        """
        self._push_enabled = push_enabled
        self._token = token
        self._channels = channels
        self._exp = exp
        self._iat = iat

    @property
    def push_enabled(self):
        """Return push_enabled"""
        return self._push_enabled

    @property
    def token(self):
        """Return token"""
        return self._token

    @property
    def channels(self):
        """Return channels"""
        return self._channels

    @property
    def exp(self):
        """Return exp"""
        return self._exp

    @property
    def iat(self):
        """Return iat"""
        return self._iat


def from_raw(raw_token):
    """
    Parse a new token from a raw token response.

    :param raw_token: Token parsed from auth response.
    :type raw_token: dict

    :return: New token model object
    :rtype: splitio.models.token.Token
    """
    if not 'pushEnabled' in raw_token or not 'token' in raw_token:
        return Token(False, None, None, None, None)
    token = raw_token['token']
    push_enabled = raw_token['pushEnabled']
    token_parts = token.strip().split('.')

    if not push_enabled or len(token_parts) < 2:
        return Token(False, None, None, None, None)

    to_decode = token_parts[1]
    decoded_token = json.loads(base64.b64decode(to_decode + '='*(-len(to_decode) % 4)))
    return Token(push_enabled, token, json.loads(decoded_token['x-ably-capability']), decoded_token['exp'], decoded_token['iat'])