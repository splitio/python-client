"""Misc decorators."""
from abc import abstractmethod


def abstract_property(func):
    """
    Abstract property decorator.

    :param func: method to decorate
    :type func: callable

    :returns: decorated function
    :rtype: callable
    """
    return property(abstractmethod(func))
