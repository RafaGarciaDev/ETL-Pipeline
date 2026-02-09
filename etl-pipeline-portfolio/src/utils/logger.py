"""
Logging Utility Module
Configures logging for the ETL pipeline.
"""

import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional


def setup_logger(
    name: str,
    log_file: Optional[str] = None,
    level: int = logging.INFO,
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Setup logger with console and file handlers.
    
    Args:
        name: Logger name
        log_file: Path to log file (None for console only)
        level: Logging level
        max_bytes: Max size of log file before rotation
        backup_count: Number of backup files to keep
        
    Returns:
        Configured logger
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Remove existing handlers
    logger.handlers = []
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if log_file specified)
    if log_file:
        # Create log directory if it doesn't exist
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


class ETLLogger:
    """
    Specialized logger for ETL operations.
    
    Provides methods for logging ETL-specific events.
    """
    
    def __init__(self, logger: logging.Logger):
        """
        Initialize ETL Logger.
        
        Args:
            logger: Base logger instance
        """
        self.logger = logger
    
    def log_extraction_start(self, source: str, params: dict = None):
        """Log start of extraction."""
        msg = f"Starting extraction from {source}"
        if params:
            msg += f" with params: {params}"
        self.logger.info(msg)
    
    def log_extraction_end(self, source: str, row_count: int, duration: float):
        """Log end of extraction."""
        self.logger.info(
            f"Extraction from {source} completed: "
            f"{row_count} rows in {duration:.2f}s"
        )
    
    def log_transformation_start(self, operation: str):
        """Log start of transformation."""
        self.logger.info(f"Starting transformation: {operation}")
    
    def log_transformation_end(
        self,
        operation: str,
        input_rows: int,
        output_rows: int
    ):
        """Log end of transformation."""
        self.logger.info(
            f"Transformation '{operation}' completed: "
            f"{input_rows} -> {output_rows} rows"
        )
    
    def log_load_start(self, target: str, row_count: int):
        """Log start of data load."""
        self.logger.info(
            f"Starting load to {target}: {row_count} rows"
        )
    
    def log_load_end(self, target: str, rows_loaded: int, duration: float):
        """Log end of data load."""
        self.logger.info(
            f"Load to {target} completed: "
            f"{rows_loaded} rows in {duration:.2f}s"
        )
    
    def log_data_quality_check(self, check_name: str, passed: bool):
        """Log data quality check result."""
        status = "PASSED" if passed else "FAILED"
        level = logging.INFO if passed else logging.WARNING
        self.logger.log(
            level,
            f"Data Quality Check '{check_name}': {status}"
        )
    
    def log_error(self, operation: str, error: Exception):
        """Log error with context."""
        self.logger.error(
            f"Error in {operation}: {str(error)}",
            exc_info=True
        )
    
    def log_pipeline_summary(
        self,
        success: bool,
        rows_processed: int,
        duration: float
    ):
        """Log pipeline execution summary."""
        status = "SUCCESS" if success else "FAILED"
        self.logger.info("=" * 80)
        self.logger.info("PIPELINE SUMMARY")
        self.logger.info(f"Status: {status}")
        self.logger.info(f"Rows Processed: {rows_processed}")
        self.logger.info(f"Duration: {duration:.2f}s")
        self.logger.info("=" * 80)
