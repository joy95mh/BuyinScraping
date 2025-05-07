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
# python -m main                 - Run all enabled spider in separate processes
# python -m main Amazon          - Run just Amazon spider

# Enable or disable markets by uncommenting or commenting
ENABLED_MARKETS = [
#     "Oleole", 
#     "Amazon",
#     "Komputronik",
    "Media Expert", # ProtonVPN
#     "Neonet",
#     "Orange",
#     "Play",
#     "Play S",
#     "Play M",
#     "Play L",
    "RTV Euro AGD", # ProtonVPN
#     "T-Mobile",
#     "XiaomiPL",
#     "Zadowolenie",
    "Mediamarkt", # ProtonVPN
#     "Morele",
#     "Plus",
#     "SamsungPL",
#     "Sferis",
#     "Vobis",
#     "x-kom",
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
    
    # Configure basic logging with a proper StreamHandler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    
    # Add the handler to the root logger
    logging.root.addHandler(console_handler)
    logging.root.setLevel(logging.INFO)
    
    return logging.getLogger(__name__)

# Load configuration
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), 'input_folder_config.json')
    print(f"Debug: Config path: {config_path}")  # Add this
    if os.path.exists(config_path):
        print("Debug: Config file exists")  # Add this
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading config: {str(e)}")
            raise
    print("Debug: Using default config")  # Add this
    if sys.platform == 'win32':
        return {
            "input_folder": r"\\SERVER\SharedFolder\BuyinScraping\input",
            "local_input_folder": r"C:\Python\Python310\projects\BuyinScraping\input",
        }
    else:
        return {
            "input_folder": "/mnt/shared/BuyinScraping/input",
            "local_input_folder": "/home/itdev/BuyinScraping/input",
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
    try:
        logger = setup_logging()
        config = load_config()
        input_paths = [
            config["input_folder"],
            config["local_input_folder"],
            get_absolute_path("input")
        ]
        print("Debug: Checking paths:", input_paths)
        input_files = []
        used_path = None
        for input_path in input_paths:
            logger.info(f"Checking input path: {input_path}")
            if not os.path.exists(input_path):
                logger.warning(f"Input path not found: {input_path}")
                continue
            for pattern in ["*.xlsm", "*.xlsx"]:
                files = glob.glob(os.path.join(input_path, pattern))
                valid_files = [f for f in files]
                if valid_files:
                    input_files.extend(valid_files)
                    used_path = input_path
                    logger.info(f"Found {len(valid_files)} Excel files in: {input_path}")
                    break
            if input_files:
                break
        if not input_files:
            logger.error("No Excel files found in any input location.")
            return None, None
        input_file = max(input_files, key=os.path.getmtime)
        return input_file, used_path
    except Exception as e:
        print(f"Error in find_input_file: {str(e)}")
        raise

def open_spider_process(market_player, input_file=None):
    """Opens a new process to run the specified market spider"""
    logger = setup_logging()
    logger.info(f"Starting {market_player} in a new process...")
    
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
    
    # Create the process for the market
    run_arg = f"_run_{market_player}"
    
    # Build command
    cmd = [sys.executable, script_path, run_arg]
    
    if sys.platform == 'win32':
        # Windows: use subprocess.Popen with appropriate settings
        process = subprocess.Popen(
            cmd,
            stdout=None,
            stderr=None,
            # Uncomment for separate console window on Windows
            # creationflags=subprocess.CREATE_NEW_CONSOLE
        )
    else:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid
        )
        print(f"Started process PID: {process.pid}")
    
    logger.info(f"Process started for {market_player}")
    return process

def execute_spider_in_current_window(market_player):
    """Executes the spider directly in the current console window"""
    logger = setup_logging()
    
    # Get the spider class
    spider_class = SPIDER_MAPPING[market_player]
    
    # Set up the spider-specific logger using the class name instead of market name
    spider_logger = setup_spider_logger(spider_class.name.lower())
    
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
    
    # Add spider to the crawler process
    if market_player in ["Play S", "Play M", "Play L"]:
        process.crawl(spider_class, input_data=filtered_data, market_player=market_player, input_file=input_file)
    else:
        process.crawl(spider_class, input_data=filtered_data, input_file=input_file)
    
    # Start the process
    process.start()
    
    logger.info(f"{market_player} spider completed")
    
    # Check the log file
    log_file_info = get_log_file_path(spider_class.name.lower())
    if log_file_info:
        logger.info(f"Detailed log available at: {log_file_info}")

def open_all_enabled_spiders():
    logger = setup_logging()
    logger.info("Launching all enabled spiders in separate processes...")

    for market in ENABLED_MARKETS:
        full_path = f"/home/itdev/BuyinScraping/buyin_run_without_vpn.sh {market}"
        try:
            logger.info(f"Launching spider for {market}...")
            subprocess.Popen(full_path, shell=True)
            time.sleep(1)  # Optional: slight delay between launches
        except Exception as e:
            logger.error(f"Failed to launch {market}: {str(e)}")

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
    
    # Ensure directories exist
    os.makedirs(daily_log_dir, exist_ok=True)
    
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
    # Example: python -m main_without_vpn Amazon
    if len(sys.argv) > 1:
        arg = sys.argv[1]

        if arg.startswith("_run_"):
            market_player = arg.replace("_run_", "")
            execute_spider_in_current_window(market_player)

        elif arg == "all":
            open_all_enabled_spiders()
        else:
            open_spider_process(arg)
    else:
        open_all_enabled_spiders()