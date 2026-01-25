"""
Logging Configuration for E-Commerce Analytics Platform

Provides structured logging with console output for debugging.
"""

import logging
import sys
from typing import Optional

import structlog
from structlog.processors import JSONRenderer, TimeStamper, CallsiteParameter
from structlog.stdlib import add_log_level, ProcessorFormatter

from src.config.settings import get_settings


def configure_logging(log_level: Optional[str] = None) -> None:
    """
    Configure structured logging for the application.
    
    Args:
        log_level: Override log level (DEBUG, INFO, WARNING, ERROR)
    """
    settings = get_settings()
    level = log_level or settings.monitoring.log_level
    
    # Convert string level to logging constant
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Common processors for all logging
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        add_log_level,
        TimeStamper(fmt="iso"),
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]
    
    # Configure structlog
    structlog.configure(
        processors=shared_processors + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Choose renderer based on environment
    if settings.monitoring.log_format == "json":
        renderer = JSONRenderer()
    else:
        # Human-readable console output
        renderer = structlog.dev.ConsoleRenderer(colors=True)
    
    # Create formatter
    formatter = ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=shared_processors,
    )
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Also configure uvicorn loggers
    for logger_name in ["uvicorn", "uvicorn.error", "uvicorn.access"]:
        logger = logging.getLogger(logger_name)
        logger.handlers = []
        logger.addHandler(console_handler)
        logger.setLevel(numeric_level)
    
    # Log that logging is configured
    log = structlog.get_logger(__name__)
    log.info(
        "Logging configured",
        level=level,
        format=settings.monitoring.log_format,
        environment=settings.app_env,
    )


def get_logger(name: str):
    """Get a logger instance with the given name."""
    return structlog.get_logger(name)
