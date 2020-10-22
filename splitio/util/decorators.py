"""Misc decorators."""
import sys

from abc import abstractmethod, abstractproperty

def abstract_property(func):
    """
    Python2/3 compatible abstract property decorator.

    :param func: method to decorate
    :type func: callable

    :returns: decorated function
    :rtype: callable
    """
    return (property(abstractmethod(func)) if sys.version_info > (3, 3)
            else abstractproperty(func))
