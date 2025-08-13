import sentry_sdk

def log_exception(message, error):
    """
    Logs the exception to your logger and reports it to Sentry.
    Args:
        message (str): Custom message to log.
        error (Exception): The exception object.
    """
    sentry_sdk.capture_exception(error)
