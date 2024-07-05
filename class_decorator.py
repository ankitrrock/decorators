import functools
from facebook.utils.exceptions.exception_handler import ExceptionHandler
from time import perf_counter
from facebook.utils.logs.logger_info import info_logger
from sentry_sdk import capture_exception

def log_decorator(func):
    """
    Decorator for logging function execution.

    :param func: Function to be decorated.
    :return: Decorated function with logging.
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        """
        Wrapper function.

        :param self: The instance of the class.
        :param args: Arguments to the function.
        :param kwargs: Keyword arguments to the function.
        :return: The result of the function execution.
        """
        if func.__name__ == '__init__':
            result = func(self, *args, **kwargs)
            return result
        else:
            try:
                time_start = perf_counter()
                result = func(self, *args, **kwargs)
                time_end = perf_counter()
                info_logger.info(f"Elapsed time during the program {func.__name__} in seconds:{time_end - time_start}")
            except Exception as error:
                capture_exception(error)
                return_type = None
                
                if func.__annotations__.get('return', None):
                    return_type = func.__annotations__['return']
                
                social_user_id = self.social_user_id
                
                try:
                    headers = self.headers
                except AttributeError:
                    headers = {}

                user_id = self.user_id
                
                result = ExceptionHandler.handle_exception(social_user_id, user_id, headers, error, return_type)
            return result
    return wrapper

class LogMeta(type):
    """
    This is a metaclass used for logging function calls in classes.
    """
    def __new__(cls, name, bases, attrs):
        """
        Modify the class's function attributes with logging.

        :param cls: The class.
        :param name: The name of the class.
        :param bases: The base classes.
        :param attrs: The class attributes.
        :return: The modified class with logging for function calls.
        """
        for attr_name, attr_value in attrs.items():
            if callable(attr_value) and not attr_name.startswith("__"):
                attrs[attr_name] = log_decorator(attr_value)
        return super(LogMeta, cls).__new__(cls, name, bases, attrs)
