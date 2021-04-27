"""Custom utilities."""


class DynamicDecorator(object):  # pylint: disable=too-few-public-methods
    """
    Decorator that will inject a decorator during class construction.

    This decorator will intercept the __init__(self, *args **kwargs) call,
    and decorate specified methods by instantiating the supplied decorators,
    with arguments extracted and mapped from the constructor call.
    For example:

    def decorator(pos_arg_1, keyword_arg_1=3):
        pass

    @DynamicDecorator(
        decorator,
        ['method1', 'method2'],
        lambda *p, **_: p[0],
        keyword_arg=lambda *_, **kw: kw.get('arg2')
    )
    class SomeClass
        def __init__(self, arg1, arg2=3:
            pass

        def method1(self, x):
            pass

        def method2(self, x):
            pass
    """

    def __init__(self, decorator, methods_to_decorate, *pos_arg_map, **kw_arg_map):
        """
        Construct a decorator with it's mappings.

        :param decorator: Original decorator to apply to specified methods.
        :type decorator: callable
        :param methods_to_decorate: List of methods (strings) where the decorator should be applied
        :type methods_to_decorate: list(string)
        :param pos_arg_map: lambdas to be called with __init__ arguments for the decorator's
            positional arguments.
        :type pos_arg_map: expanded list
        :param kw_arg_map: lambdas to be called with __init__ arguments for the decorator's keyword
            arguments.
        :type kw_arg_map: expanded dict
        """
        self._decorator = decorator
        self._methods = methods_to_decorate
        self._positional_args_lambdas = pos_arg_map
        self._keyword_args_lambdas = kw_arg_map

    def __call__(self, to_decorate):
        """
        Apply the decorator the specified class.

        :param to_decorate: Class to which the decorator will be applied.
        :type to_decorate: class

        :return: a decorated class, which inherits from `to_decorate`
        :rtype: to_decorate
        """
        decorator = self._decorator
        methods = self._methods
        positional_args_lambdas = self._positional_args_lambdas
        keyword_args_lambdas = self._keyword_args_lambdas

        class _decorated(to_decorate):  # pylint: disable=too-few-public-methods
            """
            Decorated class wrapper.

            This wrapper uses the __init__ to catch required arguments,
            instantiate the decorator with the appropriate parameters and then create a child
            class with decorated behaviour.
            """

            def __init__(self, *args, **kwargs):
                """Decorate class constructor."""
                # calculate positional and keyword arguments needed to build the decorator.
                positional = [pos_func(*args, **kwargs) for pos_func in positional_args_lambdas]
                keyword = {
                    key: func(*args, **kwargs)
                    for (key, func) in keyword_args_lambdas.items()
                }

                # call original class constructor
                to_decorate.__init__(self, *args, **kwargs)

                # decorate specified methods
                for method in methods:
                    decorated_method = decorator(*positional, **keyword)(getattr(self, method))
                    setattr(to_decorate, method, decorated_method)

        return _decorated
