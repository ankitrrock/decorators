from time import perf_counter
from facebook.utils.logs.logger_info import info_logger, error_logger
from sentry_sdk import capture_message

class LogMeta(type):
    """
    This is a metaclass used for logging function calls in classes.
    """
    def __new__(cls, cls_name, bases, attrs):
        """
        Modify the class's function attributes with logging.

        :param cls: The class.
        :param name: The name of the class.
        :param bases: The base classes.
        :param attrs: The class attributes.
        :return: The modified class with logging for function calls.
        """
        for attr_name, attr_value in attrs.items():
            if isinstance(attr_value, staticmethod):
                attrs[attr_name] = elapsed_time_wrapper(attr_value.__func__)
        return type(cls_name, bases, attrs)

def elapsed_time_wrapper(func):
    """
    Wrapper function to log payload information, elapsed time, and missing keys.

    Parameters:
        func (function): The original method/function.

    Returns:
        function: The wrapped function.
    """
    def wrapper(*args, **kwargs):
        """
        Wrapper function to calculate elapsed time and log payload information.

        Parameters:
            *args: Positional arguments for the original method.
            **kwargs: Keyword arguments for the original method.

        Returns:
            The result of the original method.
        """
        # Log the payload received
        info_logger.info(f"Payload received: {args[0]}")

        # Check for missing keys in the payload
        missing_keys = find_missing_keys(args[0])

        if not missing_keys:
            # Calculate elapsed time
            time_start = perf_counter()
            result = func(*args, **kwargs)
            time_end = perf_counter()

            # Log elapsed time
            info_logger.info(f"Elapsed time during the whole program {func.__name__} in seconds:{time_end - time_start}")
            return result
        else:
            message =f"The following keys are missing in the payload: {missing_keys}"
            capture_message(message)
            # Log missing keys
            error_logger.error(message)
            return {"status": True}

    
    return staticmethod(wrapper)

def find_missing_keys(payload):
    """
    Find missing keys in the payload.

    Parameters:
        payload (dict): The payload dictionary.

    Returns:
        list: A list of missing keys (empty list if all keys are present).
    """
    required_keys = ["user_id", "facebook_page_id", "facebook_user_id"]
    missing_keys = [key for key in required_keys if key not in payload]
    return missing_keys
