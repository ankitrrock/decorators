from facebook.utils.logs.secure_report_logs import FacebookLogs

def media_decorator(func):
    def wrapper(*args, **kwargs):
        headers = kwargs.get("headers")

        payload = kwargs.get("payload")
        
        class_name = args[0].__class__.__name__ 

        facebook_logs = FacebookLogs.create_logs_object(headers, payload)
        
        facebook_logs.create_media_started_log()

        status = facebook_logs.stop_report()

        is_media =  False 

        if status == "Stopped":
            result = is_media, "Stopped", {"status": False, "message": "NV request not triggered"}

        else: 

            # comment counter objects creation

            if class_name != "UserPosts":
                facebook_logs.create_bulk_counter_comment(kwargs.get("posts_list"))
    
            is_media, status, response = func(*args, **kwargs)

            if not response["status"]:
                if response["message"] == "NV request not triggered":
                    facebook_logs.create_media_failed_log(response["message"])

            result = is_media, status, response

        return result
    
    return wrapper
