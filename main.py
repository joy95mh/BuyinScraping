import os
import sys
import logging
import subprocess
import time
from datetime import datetime, timedelta
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging
from src.utils.file_handler import read_xlsm, filter_rows_not_updated_today, get_empty_xpath_results_rows
from src.utils.logger_setup import setup_spider_logger
from src.spiders.oleole_pl import Oleole
from src.spiders.amazon import Amazon
from src.spiders.komputronik import Komputronik
from src.spiders.media_expert import MediaExpert
from src.spiders.neonet import Neonet
from src.spiders.orange_pl import Orange
from src.spiders.play import Play
from src.spiders.play_variants import PlayVariants
from src.spiders.rtveuroagd import RtvEuroAgd
from src.spiders.tmobile import TMobile
from src.spiders.xiaomipl import XiaomiPL
from src.spiders.zadowolenie import Zadowolenie
from src.spiders.mediamarkt import Mediamarkt
from src.spiders.morele import Morele
from src.spiders.plus import Plus
from src.spiders.samsungpl import SamsungPL
from src.spiders.sferis import Sferis
from src.spiders.vobis import Vobis
from src.spiders.xkom import Xkom
import glob
import json

# Usage examples:
# python -m main                 - Run all enabled spider in separate windows
# python -m main Amazon          - Run just Amazon spider

# Enable or disable markets by uncommenting or commenting
ENABLED_MARKETS = [
    "Oleole", 
    "Amazon",
    "Komputronik",
    # "Media Expert", # ProtonVPN
    "Neonet",
    "Orange",
    "Play",
    "Play S",
    "Play M",
    "Play L",
    # "RTV Euro AGD", # ProtonVPN
    "T-Mobile",
    "XiaomiPL",
    "Zadowolenie",
    # "Mediamarkt", # ProtonVPN
    "Morele",
    # "Plus",
    "SamsungPL",
    "Sferis",
    "Vobis",
    # "x-kom",
]

# Define spider mapping
SPIDER_MAPPING = {
    "Oleole": Oleole,
    "Amazon": Amazon,
    "Komputronik": Komputronik,
    "Media Expert": MediaExpert,
    "Neonet": Neonet,
    "Orange": Orange,
    "Play": Play,
    "RTV Euro AGD": RtvEuroAgd,
    "T-Mobile": TMobile,
    "XiaomiPL": XiaomiPL,
    "Zadowolenie": Zadowolenie,
    "Mediamarkt": Mediamarkt,
    "Morele": Morele,
    "Plus": Plus,
    "SamsungPL": SamsungPL,
    "Sferis": Sferis,
    "Vobis": Vobis,
    "x-kom": Xkom,
    "Play S": PlayVariants,
    "Play M": PlayVariants,
    "Play L": PlayVariants,
}

# Setup logging for displaying proxy information
def setup_logging():
    # Disable Scrapy's logging
    configure_logging(install_root_handler=False)
    
    # Clear any existing handlers
    logging.root.handlers = []
    
    # Configure basic logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        force=True  # Force reconfiguration to prevent duplicate logging
    )
    
    # Force UTF-8 encoding for the StreamHandler (console output)
    for handler in logging.root.handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setStream(open(handler.stream.fileno(), mode='w', encoding='utf-8', errors='backslashreplace'))
    
    # Create logger
    return logging.getLogger(__name__)

# Load configuration
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), 'input_folder_config.json')
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            return json.load(f)
    return {
        "input_folder": r"\\SERVER\SharedFolder\BuyinScraping\input",  # Default network share path
        "local_input_folder": r"C:\Python\Python310\projects\BuyinScraping\input",  # Fallback local path
    }

def get_absolute_path(relative_path):
    # If running from a frozen executable
    if getattr(sys, 'frozen', False):
        # Get the directory of the executable
        application_path = os.path.dirname(sys.executable)
    else:
        # Get the directory of this script
        application_path = os.path.dirname(os.path.abspath(__file__))
        
    # Navigate up from src if needed
    if os.path.basename(application_path) == 'src':
        application_path = os.path.dirname(application_path)
        
    absolute_path = os.path.join(application_path, relative_path)
    return absolute_path

def find_input_file():
    """Find the most recent input Excel file from configured locations"""
    logger = setup_logging()
    
    # Load configuration
    config = load_config()
    input_paths = [
        config["input_folder"],  # Try network share first
        config["local_input_folder"],  # Fallback to local folder
        get_absolute_path("input")  # Final fallback to project folder
    ]
    
    # Find Excel files in the input directories
    input_files = []
    used_path = None
    
    for input_path in input_paths:
        logger.info(f"Checking input path: {input_path}")
        if not os.path.exists(input_path):
            logger.warning(f"Input path not found: {input_path}")
            continue
            
        # Look for Excel files in this path
        for pattern in ["*.xlsm", "*.xlsx"]:
            files = glob.glob(os.path.join(input_path, pattern))
            # Filter out backup files
            valid_files = [f for f in files] # if "backup" not in f.lower()
            if valid_files:
                input_files.extend(valid_files)
                used_path = input_path
                logger.info(f"Found {len(valid_files)} Excel files in: {input_path}")
                break
                
        if input_files:  # If we found files, stop looking in other paths
            break

    if not input_files:
        logger.error("No Excel files found in any input location. Please ensure:")
        logger.error(f"1. Files exist in one of these locations:")
        for path in input_paths:
            logger.error(f"   - {path}")
        logger.error("2. Files are .xlsm or .xlsx format")
        logger.error("3. Files don't contain 'backup' in their names")
        return None, None

    # Get the most recent file by modification time
    input_file = max(input_files, key=os.path.getmtime)
    logger.info(f"Using input file: {input_file}")
    logger.info(f"From location: {used_path}")
    
    return input_file, used_path

def open_spider_window(market_player, input_file=None):
    """Opens a new console window to run the specified market spider"""
    logger = setup_logging()
    logger.info(f"Starting {market_player} in a new window...")
    
    if market_player not in SPIDER_MAPPING:
        logger.error(f"Market player '{market_player}' not found. Available players: {', '.join(SPIDER_MAPPING.keys())}")
        return
    
    # If no input file provided, find it
    if input_file is None:
        input_file, _ = find_input_file()
        if not input_file:
            logger.error("No input file found! Cannot run spider.")
            return
    
    # Get this script's path for subprocess
    script_path = os.path.abspath(__file__)
    
    # Create a completely detached process for the market
    run_arg = f"_run_{market_player}"
    
    if sys.platform == 'win32':
        # Windows: use subprocess.Popen with detached flag
        cmd = [sys.executable, script_path, run_arg]
        process = subprocess.Popen(
            cmd,
            stdout=None,
            stderr=None,
            # creationflags=subprocess.CREATE_NEW_CONSOLE
        )
    else:
        # Unix: use subprocess.Popen with setsid
        cmd = [sys.executable, script_path, run_arg]
        process = subprocess.Popen(
            cmd,
            stdout=None, 
            stderr=None,
            preexec_fn=os.setsid  # Run in a new process group
        )
    
    logger.info(f"Process started for {market_player}")
    return process

def execute_spider_in_current_window(market_player):
    """Executes the spider directly in the current console window"""
    logger = setup_logging()
    # Set up the spider-specific logger too
    spider_logger = setup_spider_logger(market_player.lower().replace(" ", "_"))
    
    logger.info(f"Running {market_player} spider...")
    
    # Find input file
    input_file, _ = find_input_file()
    if not input_file:
        return
    
    # Read full dataset
    all_data = read_xlsm(input_file)
    logger.info(f"Read {len(all_data)} total rows from input file")
    
    # Filter only rows for this market player
    spider_data = []
    for row in all_data:
        if row.get("MarketPlayer", "") == market_player:
            spider_data.append(row)
    
    if not spider_data:
        logger.info(f"No rows found for {market_player}")
        return
    
    # Display sample columns from the first row to help debug
    if spider_data:
        logger.info(f"Sample column names from the data: {list(spider_data[0].keys())}")
        # Check if we can find an Xpath Result column in the first row
        xpath_columns = [col for col in spider_data[0].keys() if "xpath" in col.lower() and "result" in col.lower()]
        if xpath_columns:
            logger.info(f"Found potential Xpath Result columns: {xpath_columns}")
        else:
            logger.warning("⚠️ Could not find any column with 'xpath' and 'result' in the name!")
    
    logger.info(f"Found {len(spider_data)} total rows for {market_player}")
    
    # Get rows not updated today
    not_updated_rows = filter_rows_not_updated_today(spider_data)
    
    # Get rows with empty Xpath Result with the improved function
    empty_xpath_rows = get_empty_xpath_results_rows(spider_data)
    
    # Combine the two sets without duplicates
    filtered_data = not_updated_rows.copy()
    
    # Track which rows we've already included
    included_links = set()
    for row in not_updated_rows:
        if "PriceLink" in row:
            included_links.add(row["PriceLink"])
    
    # Debug info about empty xpath rows
    added_empty_rows = 0
    
    # Add empty xpath rows if not already included
    for row in empty_xpath_rows:
        if "PriceLink" in row and row["PriceLink"] not in included_links:
            filtered_data.append(row)
            included_links.add(row["PriceLink"])
            added_empty_rows += 1
    
    logger.info(f"Processing {len(not_updated_rows)} rows not updated today")
    logger.info(f"Found {len(empty_xpath_rows)} rows with empty Xpath Result")
    logger.info(f"Added {added_empty_rows} additional empty Xpath Result rows (not already included)")
    logger.info(f"Total rows to process: {len(filtered_data)}")
    
    if not filtered_data:
        logger.info(f"No rows to process. All rows are already updated today and have non-empty Xpath Result.")
        return
    
    # Configure Scrapy settings
    settings = get_project_settings()
    settings.set('INPUT_FILE', input_file)
    
    # Create process
    process = CrawlerProcess(settings)
    
    # Get the spider class
    spider_class = SPIDER_MAPPING[market_player]
    
    # Add spider to the crawler process
    if market_player in ["Play S", "Play M", "Play L"]:
        process.crawl(spider_class, input_data=filtered_data, market_player=market_player, input_file=input_file)
    else:
        process.crawl(spider_class, input_data=filtered_data, input_file=input_file)
    
    # Start the process
    process.start()
    
    logger.info(f"{market_player} spider completed")
    
    # Check the log file
    log_file_info = get_log_file_path(market_player.lower().replace(" ", "_"))
    if log_file_info:
        logger.info(f"Detailed log available at: {log_file_info}")

def open_all_spider_windows():
    """Opens separate console windows for all enabled market spiders with smart scheduling"""
    logger = setup_logging()
    
    # Find input file first to ensure it's passed to all spiders
    input_file, input_location = find_input_file()
    if not input_file:
        logger.error("No input file found! Cannot run spiders.")
        return
    
    logger.info(f"Using input file for all spiders: {input_file}")
    
    # Get markets to run
    markets_to_run = [m for m in ENABLED_MARKETS if m in SPIDER_MAPPING]
    
    start_time = datetime.now()
    logger.info(f"Starting all enabled markets at {start_time}")
    logger.info(f"Will run {len(markets_to_run)} markets")

    # Define the optimal number of simultaneous spiders to run
    # Use a higher number for better performance, but with safe file handling
    CONCURRENT_SPIDERS = 20  # Adjust based on your system's capacity
    
    # Start all processes with monitoring
    all_processes = []
    running_processes = []
    
    for market in markets_to_run:
        # If we already have max concurrent spiders running, wait for one to finish
        while len(running_processes) >= CONCURRENT_SPIDERS:
            # Check which processes have finished
            finished_processes = []
            for i, (m, p) in enumerate(running_processes):
                if p.poll() is not None:  # Process has finished
                    finished_processes.append(i)
                    logger.info(f"Spider {m} completed")
            
            # Remove finished processes from running list (in reverse order to avoid index issues)
            for i in sorted(finished_processes, reverse=True):
                running_processes.pop(i)
            
            # If we still have max processes running, wait a bit
            if len(running_processes) >= CONCURRENT_SPIDERS:
                time.sleep(3)  # Short delay to check again
                continue
            else:
                # Allow a short delay between finishing one spider and starting another
                # This helps prevent file access conflicts
                time.sleep(20)
                break
        
        # Start new spider process
        logger.info(f"Starting process for: {market} ({len(running_processes) + 1}/{CONCURRENT_SPIDERS} concurrent)")
        
        process = open_spider_window(market, input_file)
        if process:
            process_tuple = (market, process)
            running_processes.append(process_tuple)
            all_processes.append(process_tuple)
            
            # Small delay between starting new processes
            time.sleep(10)
    
    # Wait for all remaining processes to complete
    if running_processes:
        logger.info(f"Waiting for remaining {len(running_processes)} processes to complete...")
        
        while running_processes:
            # Check which processes have finished
            finished_processes = []
            for i, (m, p) in enumerate(running_processes):
                if p.poll() is not None:  # Process has finished
                    finished_processes.append(i)
                    logger.info(f"Spider {m} completed")
            
            # Remove finished processes
            for i in sorted(finished_processes, reverse=True):
                running_processes.pop(i)
            
            # If we still have processes running, wait
            if running_processes:
                time.sleep(10)
    
    # Calculate and show elapsed time
    end_time = datetime.now()
    elapsed = end_time - start_time
    logger.info(f"All {len(all_processes)} market processes have completed!")
    logger.info(f"Total elapsed time: {elapsed}")
    
    # Show summaries of the logs
    show_log_summaries(markets_to_run)

def get_log_file_path(spider_name):
    """Get the path to the most recent log file for a spider"""
    # Get current date for folder structure
    now = datetime.now()
    year_month = now.strftime('%Y%m')
    year_month_day = now.strftime('%Y%m%d')
    
    # Create log path
    base_log_dir = os.path.abspath(os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'logs'
    ))
    monthly_log_dir = os.path.join(base_log_dir, year_month)
    daily_log_dir = os.path.join(monthly_log_dir, year_month_day)
    
    log_file = os.path.join(daily_log_dir, f"{spider_name}.log")
    
    if os.path.exists(log_file):
        return log_file
    return None

def show_log_summaries(spider_names):
    """Show a summary of the logs for all spiders"""
    logger = setup_logging()
    logger.info("=== SPIDER RUN SUMMARIES ===")
    
    for spider in spider_names:
        spider_id = spider.lower().replace(" ", "_")
        log_file = get_log_file_path(spider_id)
        
        if log_file and os.path.exists(log_file):
            try:
                # Read the last few lines of the log file to find the completion stats
                with open(log_file, 'r', encoding='utf-8') as f:
                    # Read the last 20 lines which should include the completion summary
                    lines = f.readlines()[-20:]
                    
                # Look for the completion stats
                summary_lines = []
                in_summary = False
                for line in lines:
                    if "===================================================" in line:
                        if not in_summary:
                            in_summary = True
                            continue
                        else:
                            break
                    
                    if in_summary:
                        summary_lines.append(line.strip())
                
                if summary_lines:
                    logger.info(f"Spider: {spider}")
                    for line in summary_lines:
                        logger.info(f"  {line}")
                else:
                    logger.info(f"Spider: {spider} - No completion summary found")
            except Exception as e:
                logger.error(f"Error reading log file for {spider}: {str(e)}")
        else:
            logger.info(f"Spider: {spider} - No log file found")

if __name__ == "__main__":
    # Check if a specific market player was provided via command line
    if len(sys.argv) > 1:
        arg = sys.argv[1].strip()
        logger = setup_logging()
        logger.info(f"Command line argument: {arg}")
        
        # Check if this is a direct run request with _run_ prefix
        if arg.startswith("_run_"):
            # Extract the market player name and run directly
            market_player = arg[5:]
            logger.info(f"Running directly for market_player={market_player}")
            # When running directly, find the input file and execute the spider
            execute_spider_in_current_window(market_player)
        else:
            # Simple direct command with market player name
            market_player = arg
            logger.info(f"Opening new window for market_player={market_player}")
            # When opening a new window, find the input file here
            input_file, _ = find_input_file()
            if input_file:
                open_spider_window(market_player, input_file)
            else:
                logger.error(f"No input file found! Cannot run {market_player} spider.")
    else:
        # Default behavior: run all enabled markets in separate windows
        open_all_spider_windows() 