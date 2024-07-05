from threading import Thread
from time import perf_counter

from sentry_sdk import capture_exception, capture_message

from facebook.core.helpers.report_generation import report_generation_ms
from facebook.utils.config.config import Config
from facebook.utils.enums.post_types import PostType
from facebook.utils.exceptions.exception_handler import ExceptionHandler
from facebook.utils.kafka.confluent_kafka_api import ConfluentKafkaAPI
from facebook.utils.logs.logger_info import error_logger, info_logger
from facebook.utils.logs.secure_report_logs import FacebookLogs
from pageposts.core.page_post import PagePosts
from userposts.core.user_post import UserPosts


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
                if attr_name == "run_posts":
                    attrs[attr_name] = elapsed_time_wrapper(attr_value.__func__)
                elif attr_name in ["run_comments", "run_user_comments"]:
                    attrs[attr_name] = elapsed_time_wrapper_raw(attr_value.__func__)
        return type(cls_name, bases, attrs)


def elapsed_time_wrapper_raw(func):
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

        payload = args[0]
        headers = args[1]

        facebook_logs = FacebookLogs.create_logs_object(headers, payload)

        # Calculate elapsed time
        time_start = perf_counter()
        rate_limit, result, api_status = func(*args, **kwargs)
        time_end = perf_counter()

        if rate_limit["status"]:
            # complete log
            facebook_logs.create_or_update_counter_comment(result, True, "Success" if api_status["status"] else api_status["message"])
            platform_id = facebook_logs._platform_id
            if platform_id == 2:
                platform = "facebook_page"
                
                data = {
                        "platform": platform,
                        "social_user_id": payload.get("facebook_page_id"),
                        "report_id": facebook_logs._report_id,
                        "secure_profile_handle_id": facebook_logs._secure_profile_handle_id
                }

                headers = facebook_logs.comment_counter_headers()

                ConfluentKafkaAPI.producer_send_data(data, Config.KAFKA_TOPIC_COMMENT_COUNTER_UTILITY ,headers)

        # Log elapsed time
        info_logger.info(
            f"Elapsed time during the whole program {func.__name__} in seconds:{time_end - time_start}"
        )
        return rate_limit

    return staticmethod(wrapper)


def elapsed_time_wrapper(func):
    """
    Wrapper function to log payload information, elapsed time, and missing keys.

    Parameters:
        func (function): The original method/function.

    Returns:
        function: The wrapped function.
    """

    def wrapper(*args):
        """
        Wrapper function to calculate elapsed time and log payload information.

        Parameters:
            payload (dict): The payload dictionary.
            headers (dict): The headers dictionary.

        Returns:
            The result of the original method.
        """
        # Log the payload received
        payload = args[0]
        headers = args[1]
        info_logger.info(f"Payload received: {payload}")
        info_logger.info(f"Header received: {headers}")
        # Check for missing keys in the payload
        missing_keys = find_missing_keys(payload)

        if not missing_keys:
            # Calculate elapsed time
            time_start = perf_counter()
            try:
                result = run_secure_function(func, payload=payload, headers=headers)

            finally:
                time_end = perf_counter()

                # Log elapsed time
                info_logger.info(
                    f"Elapsed time during the whole program {func.__name__} in seconds: {time_end - time_start}"
                )

            return result
        else:
            message = f"The following keys are missing in the payload: {missing_keys}"
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
    required_keys = ["user_id", "facebook_user_id"]
    missing_keys = [key for key in required_keys if key not in payload]
    return missing_keys


def secure_decorator(func):
    """
    Decorator to log account post status and handle exceptions during data collection.

    Parameters:
        func (function): The original method/function.

    Returns:
        function: The wrapped function.
    """

    def wrapper(*args, **kwargs):
        """
        Wrapper function to log account post status and handle exceptions during data collection.

        Parameters:
            *args: Positional arguments for the original method.
            **kwargs: Keyword arguments for the original method.

        Returns:
            The result of the original method.
        """
        headers = kwargs.get("headers")
        payload = kwargs.get("payload")

        page_id = payload.get("facebook_page_id")
        user_id = payload.get("user_id")
        facebook_user_id = payload.get("facebook_user_id")
        if page_id:
            posts = PagePosts(
                page_access_token="",
                page_id=page_id,
                user_id=user_id,
                facebook_user_id=facebook_user_id,
                monitoring_date="",
                headers=headers,
            )
        else:
            posts = UserPosts(
                user_access_token="",
                page_id=page_id,
                user_id=user_id,
                social_user_id=facebook_user_id,
                headers=headers,
            )

        # Create FacebookLogs object for secure logging
        facebook_logs = FacebookLogs.create_logs_object(headers, payload)

        # Log the start of the data collection
        facebook_logs.create_accountpost_startedlogs(PostType.DATA_COLLECTION.value)

        rate_limit = {"status": True}
        social_handle_parameters = (
            headers.get("social_handle_parameters").split(",")
            if headers.get("social_handle_parameters", None)
            else []
        )
        try:
            
            token_verification, result, is_media, stop_status, rate_limit = func(
                *args, **kwargs
            )

            if rate_limit["status"]:
                if stop_status != "Stopped":
                    if len(result) == 0:
                        if token_verification["status"]:
                            # Log the successful data collection (0 posts)
                            facebook_logs.create_accountpost_completelogs(
                                PostType.DATA_COLLECTION.value,
                                zero_post=True,
                                message="zero posts found",
                            )
                        else:
                            # Log the failed data collection with the specific error message
                            facebook_logs.create_accountpost_failedlogs(
                                job_type=PostType.DATA_COLLECTION.value,
                                message=token_verification["message"],
                            )
                            posts.update_sql_db(status=False)
                        call_report_gen(facebook_logs)
                    elif token_verification["status"]:
                        # media analysis trigger if no media found
                        if not is_media:
                            media_analysis_status = (
                                facebook_logs.send_media_analysis_request()
                            )
                            if media_analysis_status:
                                # complete logs
                                facebook_logs.create_accountpost_completelogs(
                                    PostType.DATA_COLLECTION.value, data=result
                                )
                               
                            else:
                                # failed logs
                                call_report_gen(facebook_logs)

                            if page_id:
                                
                                if (
                                    "Facebook Page Comments" in social_handle_parameters
                                    or len(social_handle_parameters) == 0
                                ):
                                    # Log the start of the comment data collection
                                    facebook_logs.create_accountpost_startedlogs(PostType.DATA_COLLECTION.value, "Facebook Page Comments")

                        else:
                            facebook_logs.create_accountpost_completelogs(
                                PostType.DATA_COLLECTION.value, data=result
                            )

                            if page_id:
                                if (
                                    "Facebook Page Comments" in social_handle_parameters
                                    or len(social_handle_parameters) == 0
                                ):
                                    # Log the start of the comment data collection
                                    facebook_logs.create_accountpost_startedlogs(PostType.DATA_COLLECTION.value, "Facebook Page Comments")

                # stop logs
                else:
                    facebook_logs.create_accountpost_stoplogs(message=stop_status)
                    posts.update_sql_db(status=False)

        except Exception as e:
            capture_exception(e)
            # Log the failed data collection with the specific error message
            facebook_logs.create_accountpost_failedlogs(
                job_type=PostType.DATA_COLLECTION.value, message=str(e)
            )
            posts.update_sql_db(status=False)
            call_report_gen(facebook_logs)
            ExceptionHandler.handle_general_exception(
                payload.get("facebook_user_id"), payload.get("user_id"), headers, e
            )
            rate_limit = {"status": True}

        return rate_limit

    return wrapper


@secure_decorator
def run_secure_function(func, payload, headers):
    """
    Function to perform the actual secure data collection.

    Parameters:
        func (function): The original method/function.
        payload (dict): The payload dictionary.
        headers (dict): The headers dictionary.

    Returns:
        The result of the data collection.
    """
    return func(payload, headers)


def call_report_gen(facebook_logs):
    """
    Function to call report generation.

    Parameters:
        facebook_logs (obj): FacebookLogs object.
    """
    if facebook_logs._report_id:
        microservice_model = {
            "report_id": facebook_logs._report_id,
            "report_type": facebook_logs._report_type,
            "secure_profile_handle_id": facebook_logs._secure_profile_handle_id,
        }
        path = (
            "facebook-user-analytic"
            if facebook_logs._platform_id == 11
            else "facebook-page-analytic"
        )
        Thread(
            target=report_generation_ms,
            args=(path, microservice_model),
        ).start()
