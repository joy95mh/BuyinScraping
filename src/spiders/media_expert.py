from src.spiders.base_spider import BaseSpider
from src.items import ProductItem
from src.utils.price_formatter import format_pl_price
import scrapy
import logging
import json
import random
import time
import requests
from lxml import html
import re
import urllib3
import ssl
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# Disable SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Set selenium logging to WARNING level to reduce verbosity
selenium_logger = logging.getLogger('selenium')
selenium_logger.setLevel(logging.WARNING)

# Also set other noisy loggers to WARNING
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('selenium.webdriver.remote.remote_connection').setLevel(logging.WARNING)
logging.getLogger('selenium.webdriver.remote').setLevel(logging.WARNING)
logging.getLogger('selenium.webdriver').setLevel(logging.WARNING)

class MediaExpert(BaseSpider):
    name = "mediaexpert"
    market_player = "Media Expert"
    use_proxy_manager = False 
    use_human_like_delay = True

    # Custom sleep time range for human-like delays
    min_sleep_time = 2  # Minimum sleep time in seconds (reduced for performance)
    max_sleep_time = 4  # Maximum sleep time in seconds (reduced for performance)
    page_load_timeout = 25  # Timeout for page loading in seconds
    wait_timeout = 8  # Timeout for waiting for elements (reduced for performance)
    
    # Number of products to process before browser refresh
    # This helps manage memory usage
    browser_refresh_interval = 10

    # Success indicator phrases
    success_indicators = [
        'W koszyku',
        'Dodaj do koszyka',
        'Do koszyka',
        'Kup teraz',
        'cena-',
        'Specyfikacja',
        'Cechy produktu',
        'Opis produktu',
    ]

    custom_settings = {
        **BaseSpider.custom_settings,
        'DOWNLOAD_DELAY': 3,  # Reduced for better performance
        'CONCURRENT_REQUESTS': 1,
        'HTTPERROR_ALLOWED_CODES': [403, 404, 429, 503],
        'LOG_LEVEL': 'INFO',
    }
    
    # Polish user agents
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
    ]

    def __init__(self, input_data=None, *args, **kwargs):
        super().__init__(input_data, *args, **kwargs)
        
        # Store product results
        self.processed_products = []
        
        # Setup product queue
        self.product_queue = []
        
        # WebDriver instance
        self.driver = None
        
        # Track if driver is initialized
        self.driver_initialized = False
        
        # Counter for browser refresh
        self.products_processed = 0

    def get_random_ua(self):
        """Get a random user agent from our list"""
        return random.choice(self.user_agents)
    
    def init_webdriver(self):
        """Initialize the WebDriver for Selenium with optimized settings"""
        if self.driver_initialized:
            return True
            
        try:
            self.log_info("Initializing Chrome WebDriver...")
            
            # Suppress stdout and stderr from the driver
            os.environ['WDM_LOG_LEVEL'] = '0'
            os.environ['WDM_PRINT_FIRST_LINE'] = 'False'
            
            # Configure Chrome options
            chrome_options = Options()
            chrome_options.add_argument("--headless")  # Run in headless mode
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--window-size=1366,768")  # Smaller window size for less memory
            chrome_options.add_argument("--log-level=3")  # Silence Chrome's console output
            chrome_options.add_argument("--silent")
            
            # Performance optimizations
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-browser-side-navigation")
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--disable-features=IsolateOrigins,site-per-process")
            chrome_options.add_argument("--disable-site-isolation-trials")
            chrome_options.add_argument("--disable-features=NetworkService")
            chrome_options.add_argument("--blink-settings=imagesEnabled=false")  # Disable images
            
            # Disable logging
            chrome_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
            
            # Use a random user agent
            user_agent = self.get_random_ua()
            chrome_options.add_argument(f"--user-agent={user_agent}")
            
            # Add language settings for Poland
            chrome_options.add_argument("--lang=pl-PL")
            chrome_options.add_argument("--accept-lang=pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7")
            
            # Privacy settings to avoid tracking
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("useAutomationExtension", False)
            
            # Set a small cache and disk cache size to reduce memory usage
            chrome_options.add_argument("--disk-cache-size=1000000")
            chrome_options.add_argument("--media-cache-size=1000000")
            
            # Initialize the WebDriver
            self.driver = webdriver.Chrome(options=chrome_options)
            
            # Set page load timeout
            self.driver.set_page_load_timeout(self.page_load_timeout)
            
            # Execute CDP commands to modify navigator properties
            self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['pl-PL', 'pl', 'en-US', 'en']
                });
                """
            })
            
            self.driver_initialized = True
            self.log_info("WebDriver initialized successfully")
            return True
            
        except Exception as e:
            self.log_error(f"Failed to initialize WebDriver: {str(e)}")
            return False
    
    def refresh_webdriver(self):
        """Refresh the WebDriver to prevent memory leaks"""
        self.log_info("Refreshing Chrome WebDriver to prevent memory leaks")
        try:
            if self.driver:
                self.driver.quit()
            self.driver = None
            self.driver_initialized = False
            return self.init_webdriver()
        except Exception as e:
            self.log_error(f"Error refreshing WebDriver: {str(e)}")
            return False
    
    def close_webdriver(self):
        """Close the WebDriver when done"""
        if self.driver:
            try:
                self.log_info("Closing WebDriver...")
                self.driver.quit()
            except Exception as e:
                self.log_error(f"Error closing WebDriver: {str(e)}")
            finally:
                self.driver = None
                self.driver_initialized = False
    
    def start_requests(self):
        """Start the scraping process by initializing WebDriver and processing products"""
        self.log_info(f"🕷️ Starting {self.market_player} spider with optimized Selenium WebDriver")
        
        # Filter rows that match our market player
        product_rows = [row for row in self.input_data if row["MarketPlayer"] == self.market_player]
        
        if not product_rows:
            self.log_info(f"⚠️ No rows found for {self.market_player}")
            return
            
        self.log_info(f"🔍 Total products to scrape: {len(product_rows)}")
        self.product_queue = product_rows
        
        # Initialize WebDriver
        if not self.init_webdriver():
            self.log_error("Cannot proceed without WebDriver")
            return
            
        # Start processing the first product
        if self.product_queue:
            yield self.get_next_product_request()
        
    def get_next_product_request(self):
        """Get the next product to process and create a request for it"""
        if not self.product_queue:
            return None
            
        # Get the next product
        row = self.product_queue.pop(0)
        url = row["PriceLink"]
        
        # Create a request that will directly process the product
        return scrapy.Request(
            url=url,
            callback=self.parse_product,
            meta={
                "row": row,
                "dont_redirect": True,
                "handle_httpstatus_list": [403, 429, 503],
            },
            dont_filter=True
        )
            
    def parse_product(self, response):
        """Process a product using Selenium WebDriver"""
        # Get product information from meta
        row = response.meta["row"]
        url = row["PriceLink"]
        url_id = row.get("BNCode", "unknown")
            
        self.log_info(f"Processing product: {url_id} - URL: {url}")
        
        # Add human-like delay
        if self.use_human_like_delay:
            sleep_time = random.uniform(self.min_sleep_time, self.max_sleep_time)
            self.log_info(f"😴 Sleeping for {sleep_time:.2f} seconds before processing {url_id}")
            time.sleep(sleep_time)
        
        # Check if WebDriver is initialized
        if not self.driver_initialized and not self.init_webdriver():
            self.log_error(f"Cannot process {url_id} without WebDriver")
            item = self.create_default_item(row)
            yield item
            
            # Continue with next product
            if self.product_queue:
                yield self.get_next_product_request()
            else:
                self.close_webdriver()
            return
            
        # Refresh browser after certain number of products to manage memory
        self.products_processed += 1
        if self.products_processed % self.browser_refresh_interval == 0:
            self.log_info(f"Refreshing browser after {self.browser_refresh_interval} products")
            self.refresh_webdriver()
        
        # Process with WebDriver
        success = False
        
        try:
            # Load the page with WebDriver
            self.log_info(f"Loading {url} with WebDriver...")
            self.driver.get(url)
            
            # Wait for page to load - look for either product content or error indicators
            try:
                # Wait for a common element on the product page
                # This uses explicit wait to check for either success or failure
                WebDriverWait(self.driver, self.wait_timeout).until(
                    lambda d: any(indicator in d.page_source for indicator in self.success_indicators) or 
                              "produkt niedostępny" in d.page_source.lower()
                )
                
                # If we got here, the page loaded successfully
                self.log_info(f"Page for {url_id} loaded successfully")
                success = True
                
            except TimeoutException:
                self.log_warning(f"Timeout waiting for page elements for {url_id}")
                
                # Check if we have any product information despite the timeout
                if any(indicator in self.driver.page_source for indicator in self.success_indicators):
                    self.log_info(f"Found some product information despite timeout for {url_id}")
                    success = True
        
        except WebDriverException as e:
            self.log_error(f"WebDriver error for {url_id}: {str(e)}")
            
        # Create a product item based on the result
        if success:
            item = self.extract_product_info(row, self.driver.page_source)
        else:
            item = self.create_default_item(row)
            
        # Yield the item
        yield item
        
        # Continue with next product
        if self.product_queue:
            yield self.get_next_product_request()
        else:
            self.log_info(f"✅ Finished processing all products")
            self.close_webdriver()
    
    def extract_product_info(self, row, page_source):
        """Extract product information from the page source"""
        url = row["PriceLink"]
        url_id = row.get("BNCode", "unknown")
        
        # Create default item
        item = self.create_default_item(row)
        
        try:
            # Parse the HTML
            tree = html.fromstring(page_source)
            
            price = ""
            stock_status = ""
            json_availability = False

            # Try to get price from JSON-LD data first
            json_ld_elements = tree.xpath("//script[@type='application/ld+json' and contains(text(),'priceCurrency')]/text()")
            if json_ld_elements:
                try:
                    data = json.loads(json_ld_elements[0])
                    price = 'offers' in data and data['offers']['price'] or ""
                    price = self.fix_price(str(price))
                    
                    # Check if availability indicates out of stock
                    if 'offers' in data and 'availability' in data['offers']:
                        json_availability = True
                        avail = data['offers']['availability']
                        if 'OutOfStock' in avail:
                            stock_status = "Outstock"
                            price = "0.00"
                        else:
                            stock_status = "Instock"
                except Exception as e:
                    self.log_error(f"Error parsing JSON-LD: {str(e)}")
            
            if not price:
                # Try multiple price selectors
                price_selectors = [
                    "//meta[@property='product:price:amount']/@content",
                    "//span[contains(@class, 'price') and contains(@class, 'value')]/text()",
                    "//div[contains(@class, 'main-price')]//span[@class='value']/text()",
                    "//div[@data-price]/@data-price",
                    "//form[@id='product-offers-form']//span[contains(@class, 'price')]/text()"
                ]
                
                for selector in price_selectors:
                    price_elements = tree.xpath(selector)
                    if price_elements:
                        raw_price = price_elements[0]
                        price = self.fix_price(raw_price)
                        if price:
                            break

            # Update these XPath selectors based on Media Expert's out-of-stock indicators
            if not stock_status:
                out_of_stock_selectors = [
                    "//meta[@property='product:availability'][@content='notavailable']",
                    "//div[@class='offer-unavailable is-product-show']",
                    "//span[contains(text(), 'produkt będzie dostępny')]",
                    "//div[contains(@class, 'unavailable-box')]",
                    "//div[contains(text(), 'Produkt niedostępny')]",
                    "//div[contains(@class, 'product-unavailable')]",
                    "//span[@class='unavailable is-regular']",
                ]
                
                is_out_of_stock = any(len(tree.xpath(selector)) > 0 for selector in out_of_stock_selectors)
                
                if is_out_of_stock:
                    stock_status = "Outstock"
                    price = "0.00"
                elif not is_out_of_stock and not json_availability:
                    stock_status = "Outstock"
                    price = "0.00"
                else:
                    stock_status = "Instock"

            
            self.log_info(f"Price: {price} PLN , Stock: {stock_status}, url: {url}")
            
            # Update the item with actual values
            item["xpath_result"] = price
            item["out_of_stock"] = stock_status
            
            return item
            
        except Exception as e:
            self.log_error(f"⚠️ Error processing {url_id}: {str(e)}")
            return item
    
    def create_default_item(self, row):
        """Create a default product item with empty values"""
        url = row["PriceLink"]
        
        item = ProductItem()
        item["price_link"] = url
        item["xpath_result"] = "0.00"
        item["out_of_stock"] = "Outstock"
        item["market_player"] = self.market_player
        if "BNCode" in row:
            item["bn_code"] = row["BNCode"]
            
        return item
    
    def fix_price(self, price_text):
        """Helper method to clean price string"""
        if price_text:
            return format_pl_price(price_text)
        return ""
    
    def parse(self, response):
        """Default parse method"""
        row = {"PriceLink": response.url, "MarketPlayer": self.market_player}
        if "row" in response.meta:
            row = response.meta["row"]
            
        return self.parse_product(response)
        
    def __del__(self):
        """Ensure WebDriver is closed when spider is garbage collected"""
        self.close_webdriver()