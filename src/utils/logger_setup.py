import os
import logging
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler

def setup_spider_logger(spider_name):
    """
    Configure a logger that writes to a file in the logs directory organized by date.
    
    Directory structure will be:
    logs/YYYYMM/YYYYMMDD/spider_name.log
    
    Args:
        spider_name: Name of the spider to use for the log file
        
    Returns:
        A configured logger instance
    """
    # Get current date for folder structure
    now = datetime.now()
    year_month = now.strftime('%Y%m')
    year_month_day = now.strftime('%Y%m%d')
    
    # Create logs directory structure
    base_log_dir = os.path.abspath(os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'logs'
    ))
    monthly_log_dir = os.path.join(base_log_dir, year_month)
    daily_log_dir = os.path.join(monthly_log_dir, year_month_day)
    
    # Create directories if they don't exist
    for directory in [base_log_dir, monthly_log_dir, daily_log_dir]:
        if not os.path.exists(directory):
            os.makedirs(directory)
    
    # Setup log file path
    log_file = os.path.join(daily_log_dir, f"{spider_name}.log")
    
    # Configure logger with our custom format that includes elapsed time
    logger = logging.getLogger(spider_name)
    
    # Don't add handlers if they already exist
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Create file handler
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB max file size
            backupCount=5,
            encoding='utf-8'  # Ensure UTF-8 encoding for file logging
        )
        
        # Also create a console handler for terminal output
        console_handler = logging.StreamHandler()
        
        # Fix emoji encoding issues by ensuring UTF-8 for console output
        try:
            import codecs
            import sys
            console_stream = codecs.getwriter('utf-8')(sys.stdout.buffer, 'backslashreplace')
            console_handler.setStream(console_stream)
        except Exception:
            # Fallback if the above method doesn't work
            pass
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s')
        
        # Add formatter to handlers
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        # Prevent propagation to root logger
        logger.propagate = False
    
    return logger

def log_final_stats(logger, spider_name, processed_count, start_time):
    """
    Log the final statistics for a spider run
    
    Args:
        logger: The logger instance
        spider_name: Name of the spider
        processed_count: Number of items processed
        start_time: Start time of the spider run (time.time() value)
    """
    # Calculate duration
    end_time = time.time()
    total_duration = end_time - start_time
    
    # Format duration nicely
    hours, remainder = divmod(total_duration, 3600)
    minutes, seconds = divmod(remainder, 60)
    duration_str = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"
    
    # Calculate items per minute
    items_per_minute = 0
    if total_duration > 0 and processed_count > 0:
        items_per_minute = (processed_count * 60) / total_duration
    
    # Log summary
    logger.info("=" * 50)
    logger.info(f"SPIDER RUN COMPLETED: {spider_name}")
    logger.info(f"Items processed: {processed_count}")
    logger.info(f"Duration: {duration_str}")
    logger.info(f"Performance: {items_per_minute:.2f} items/minute")
    logger.info("=" * 50) 