"""Unit test helpers"""
from __future__ import absolute_import, division, print_function, unicode_literals

from sys import version_info

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from string import ascii_letters, digits, printable
from random import choice


class MockUtilsMixin(object):
    """Add handy methods to reduce boilerplate on tests that patch things on initialization.
    Example usage:

    class MyTest(object, MockUtilsMixin):
        def setUp(self):
            self.obj = ObjClass()
            self.some_mock = self.patch('some.module.function')
            self.some_function = self.patch_object(self.obj, 'some_method')
    """
    def patch_builtin(self, name, *args, **kwargs):
        return self.patch(('builtins.{}' if version_info >= (3,) else '__builtin__.{}').format(
            name), *args, **kwargs)

    def patch(self, *args, **kwargs):
        patcher = mock.patch(*args, **kwargs)
        patched = patcher.start()
        patched.patcher = patcher
        self.addCleanup(patcher.stop)
        return patched

    def patch_object(self, *args, **kwargs):
        patcher = mock.patch.object(*args, **kwargs)
        patched = patcher.start()
        patched.patcher = patcher
        self.addCleanup(patcher.stop)
        return patched


def random_alphanumeric_string(size):
    """
    Generates a random alphanumeric string of a given size
    :param size: The size of the string
    :type size: int
    :return: An alphanumeric string
    :rtype: str
    """
    return [choice(ascii_letters + digits) for _ in range(size)]


def random_printable_string(size):
    """
    Generates a random printable string of a given size
    :param size: The size of the string
    :type size: int
    :return: A printable string
    :rtype: str
    """
    return [choice(printable) for _ in range(size)]
