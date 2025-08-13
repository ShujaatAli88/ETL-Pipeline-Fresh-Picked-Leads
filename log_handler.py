from loguru import logger
from pathlib import Path
import sys

# Configuration
log_dir = Path("logs")
log_file = log_dir / "latest.log"
level = "INFO"

# Ensure log directory exists
log_dir.mkdir(parents=True, exist_ok=True)

# Try to remove the previous log file only
if log_file.exists():
    try:
        log_file.unlink()
    except PermissionError:
        pass  # If file is in use, skip deleting

# Remove any default loggers
logger.remove()

# Console logger
logger.add(
    sink=sys.stdout,
    level=level,
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
           "<level>{level: <8}</level> | "
           "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
           "<level>{message}</level>"
)

# File logger
logger.add(
    sink=log_file,
    level=level,
    encoding="utf-8",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}"
)

logger.info(f"📝 Logging initialized. Writing to: {log_file.resolve()}")