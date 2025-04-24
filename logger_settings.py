from pydantic import BaseModel
import enum
from typing import Any

class LogLevel(enum.Enum):
    """
    Enum for log levels.
    """
    TRACE = "TRACE"
    DEBUG = "DEBUG"
    INFO = "INFO"
    SUCCESS = "SUCCESS"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

def format_assigner(denomination):
    """
    Function to assign the format for the logger.
    docs: https://loguru.readthedocs.io/en/stable/api/logger.html#loguru._logger.Logger.add
    """
    time = "<fg #808080>{time:YYYY/MM/DD HH:mm:ss}</> | "
    logger_name = f"<fg #FFD700>{denomination}</> | "
    level = "<level>{level: <8}</level> | "
    location = "<cyan>{name}:{function}:{line}</cyan> - "
    message = "<level>{message}</level>"
    return time + logger_name + level + location + message

class LoggerSettings(BaseModel):
    """
    Default logger settings.
    """
    denomination:str = "LOGGER"
    level: str = LogLevel.DEBUG.value
    format: str = None
    colorize: bool = True
    serialize:bool = False
    catch:bool = False

    def model_post_init(self, __context: Any) -> None:
        if not self.format:
            self.format = format_assigner(self.denomination)


STDOUT_LOGGER_SETTINGS = LoggerSettings(denomination="STDOUT", level=LogLevel.INFO.value)
DB_STDOUT_LOGGER_SETTINGS = LoggerSettings(denomination="DB+OUT", level=LogLevel.INFO.value)
DB_LOGGER_SETTINGS = LoggerSettings(denomination="DB+OUT", level=LogLevel.INFO.value, colorize=False, serialize=True, catch=True)
DB_ONLY_LOGGER_SETTINGS = LoggerSettings(denomination="DB_ONLY", level=LogLevel.INFO.value, colorize=False, serialize=True, catch=True)