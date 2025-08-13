import os, atexit, logging
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration

from fresh_picked_leads import main as main_fresh_leads
from log_handler import logger
from config import Config

sentry_sdk.init(
    dsn=Config.SENTRY_DSN,
    environment=os.getenv("ENVIRONMENT", "prod"),
    release=os.getenv("RELEASE", "freshpickedleads@0.1.0"),
    integrations=[LoggingIntegration(level=logging.INFO, event_level=logging.ERROR)],
    send_default_pii=False
)
atexit.register(lambda: sentry_sdk.flush(timeout=10))

def main():
    logger.info("Starting Fresh Picked Leads.")
    main_fresh_leads()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("Top-level crash")
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush(timeout=10)
        raise
