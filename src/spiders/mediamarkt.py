from src.spiders.base_spider import BaseSpider
from src.items import ProductItem
from src.utils.proxy_manager import ProxyManagerFreeProxyListDotNet
from src.utils.price_formatter import format_pl_price
import scrapy
import logging
import json
import random
import time
import re
import os
import sys
import subprocess
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from lxml import html
import urllib3

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

class Mediamarkt(BaseSpider):
    name = "mediamarkt"
    market_player = "Mediamarkt"
    use_proxy_manager = False
    use_human_like_delay = True
    
    # Custom sleep time range for human-like delays
    min_sleep_time = 1.5  # Minimum sleep time in seconds
    max_sleep_time = 3.0  # Maximum sleep time in seconds
    page_load_timeout = 20  # Timeout for page loading in seconds
    wait_timeout = 8  # Timeout for waiting for elements
    
    # Use undetected_chromedriver for better CloudFlare bypassing
    use_undetected_chromedriver = True
    
    # Number of products to process before browser refresh
    browser_refresh_interval = 5
    
    # Success indicator phrases for MediaMarkt
    success_indicators = [
        'Dodaj do koszyka',
        'Do koszyka',
        'W koszyku',
        'Specyfikacja',
        'cofr-price',
        'Opis produktu',
        'pdp-add-to-cart-button',
        'Kup teraz',
    ]
    
    # Consent dialog button selectors
    consent_button_selectors = [
        "//button[contains(text(), 'Akceptuję wszystkie')]",
        "//button[contains(text(), 'Akceptuję wszystko')]",
        "//button[contains(@class, 'btn-accept-all')]",
        "//button[contains(text(), 'Accept all')]",
    ]

    custom_settings = {
        **BaseSpider.custom_settings,
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 1,
        'HTTPERROR_ALLOWED_CODES': [403, 404, 429, 503],
        'LOG_LEVEL': 'INFO',
    }
    
    # Polish user agents
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    ]
    
    # Working headers from successful browser session
    @staticmethod
    def get_working_headers():
        return {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'max-age=0',
            'priority': 'u=0, i',
            'referer': 'https://mediamarkt.pl/komputery-i-tablety/router-tcl-link-zone-mw63vk-4g-lte-cat-6-czarny?__cf_chl_tk=0DYipilBFZ4.9ErJiD6LEcv6w6wGQWqPqjkSaknau_U-1746579399-1.0.1.1-l5y4x68q.ffHIoDwhwn8SPrHJBKD9qsC2ETHaZqacKM',
            'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            'sec-ch-ua-arch': '"x86"',
            'sec-ch-ua-bitness': '"64"',
            'sec-ch-ua-full-version': '"135.0.7049.115"',
            'sec-ch-ua-full-version-list': '"Google Chrome";v="135.0.7049.115", "Not-A.Brand";v="8.0.0.0", "Chromium";v="135.0.7049.115"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-model': '""',
            'sec-ch-ua-platform': '"Windows"',
            'sec-ch-ua-platform-version': '"19.0.0"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
        }
        
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
        
        # Check if undetected_chromedriver dependency is available
        if self.use_undetected_chromedriver:
            self.install_undetected_chromedriver()
    
    def install_undetected_chromedriver(self):
        """Install undetected_chromedriver if not available"""
        try:
            import undetected_chromedriver as uc
            self.log_info("undetected_chromedriver is already installed")
        except ImportError:
            try:
                self.log_info("Installing undetected_chromedriver...")
                subprocess.check_call([sys.executable, "-m", "pip", "install", "undetected-chromedriver"])
                self.log_info("undetected_chromedriver installed successfully")
            except Exception as e:
                self.log_error(f"Failed to install undetected_chromedriver: {str(e)}")
                self.use_undetected_chromedriver = False

    def init_webdriver(self):
        """Initialize the WebDriver with direct product access settings"""
        if self.driver_initialized:
            return True
            
        if self.use_undetected_chromedriver:
            try:
                import undetected_chromedriver as uc
                self.log_info("Initializing undetected_chromedriver for MediaMarkt...")
                
                # Configure Chrome options
                chrome_options = uc.ChromeOptions()
                chrome_options.add_argument("--window-size=1920,1080")
                
                # Get working headers to match
                headers = self.get_working_headers()
                
                # Add language settings for Poland
                chrome_options.add_argument("--lang=pl-PL")
                
                # Add referrer
                chrome_options.add_argument(f"--referrer={headers['referer']}")
                
                # Use the user agent from headers
                chrome_options.add_argument(f"--user-agent={headers['user-agent']}")
                
                # Create driver with undetected_chromedriver
                self.driver = uc.Chrome(options=chrome_options)
                
                # Set timeout
                self.driver.set_page_load_timeout(self.page_load_timeout)
                
                # Configure headers via CDP
                self.driver.execute_cdp_cmd('Network.setExtraHTTPHeaders', {'headers': headers})
                
                # Apply anti-detection JavaScript
                self.apply_anti_detection_js()
                
                self.driver_initialized = True
                self.log_info("WebDriver initialized successfully with undetected_chromedriver")
                return True
                
            except Exception as e:
                self.log_error(f"Failed to initialize undetected_chromedriver: {str(e)}")
                # Fall back to regular chromedriver
                self.use_undetected_chromedriver = False
                
        # Regular Chrome WebDriver
        try:
            self.log_info("Initializing regular Chrome WebDriver for MediaMarkt...")
            
            # Configure Chrome options
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            
            # Get working headers
            headers = self.get_working_headers()
            
            # Add language and referrer
            chrome_options.add_argument("--lang=pl-PL")
            chrome_options.add_argument(f"--referrer={headers['referer']}")
            chrome_options.add_argument(f"--user-agent={headers['user-agent']}")
            
            # Disable logging and automation flags
            chrome_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Initialize the WebDriver
            self.driver = webdriver.Chrome(options=chrome_options)
            
            # Set page load timeout
            self.driver.set_page_load_timeout(self.page_load_timeout)
            
            # Configure headers via CDP
            self.driver.execute_cdp_cmd('Network.setExtraHTTPHeaders', {'headers': headers})
            
            # Apply anti-detection JavaScript
            self.apply_anti_detection_js()
            
            self.driver_initialized = True
            self.log_info("WebDriver initialized successfully")
            return True
            
        except Exception as e:
            self.log_error(f"Failed to initialize WebDriver: {str(e)}")
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
                self.driver = None
            return False
    
    def apply_anti_detection_js(self):
        """Apply JavaScript modifications to avoid detection"""
        try:
            if not self.driver:
                return
                
            # Execute CDP commands to modify properties
            self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                // Hide automated browser features
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                
                // Add plugins that a normal browser would have
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [
                        {
                            0: {type: "application/pdf"},
                            description: "PDF Viewer",
                            filename: "internal-pdf-viewer",
                            length: 1,
                            name: "Chrome PDF Viewer"
                        },
                        {
                            0: {type: "application/x-google-chrome-pdf"},
                            description: "Portable Document Format",
                            filename: "mhjfbmdgcfjbbpaeojofohoefgiehjai",
                            length: 1,
                            name: "Chrome PDF Plugin"
                        }
                    ]
                });
                
                // Set Polish language to match headers
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['pl-PL', 'pl', 'en-US', 'en']
                });
                
                // Set realistic hardware concurrency
                Object.defineProperty(navigator, 'hardwareConcurrency', {
                    get: () => 8
                });
                """
            })
            
        except Exception as e:
            self.log_warning(f"Error applying anti-detection JS: {str(e)}")
    
    def handle_consent_dialog(self):
        """Handle the cookie consent dialog if present"""
        try:
            if not self.driver:
                return False
                
            # Check if any consent button exists
            for selector in self.consent_button_selectors:
                try:
                    consent_buttons = self.driver.find_elements(By.XPATH, selector)
                    if consent_buttons and len(consent_buttons) > 0:
                        self.log_info(f"Found consent dialog button with selector: {selector}")
                        
                        # Click the first matching button
                        button = consent_buttons[0]
                        
                        # Scroll to the button
                        self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", button)
                        time.sleep(0.5)
                        
                        # Try clicking the button
                        try:
                            button.click()
                            self.log_info("Clicked consent button")
                            time.sleep(1)  # Wait for the dialog to disappear
                            return True
                        except:
                            # Try JavaScript click if normal click fails
                            self.driver.execute_script("arguments[0].click();", button)
                            self.log_info("Clicked consent button using JavaScript")
                            time.sleep(1)
                            return True
                except Exception as e:
                    self.log_warning(f"Error with consent button selector {selector}: {str(e)}")
                    
            # If we get here, we didn't find any consent buttons
            return False
                
        except Exception as e:
            self.log_warning(f"Error handling consent dialog: {str(e)}")
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
        self.log_info(f"🕷️ Starting {self.market_player} spider with Selenium WebDriver")
        
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
                "handle_httpstatus_list": [403, 404, 429, 503],
            },
            dont_filter=True
        )
    
    def element_exists(self, tree, xpath):
        """Check if element exists in lxml tree"""
        return len(tree.xpath(xpath)) > 0
        
    def fix_price(self, price_text):
        """Helper method to clean price string"""
        if price_text:
            return format_pl_price(price_text)
        return ""
        
    def simulate_human_behavior(self):
        """Simulate human-like behavior with scrolling"""
        try:
            # Skip if no driver
            if not self.driver:
                return
                
            # Random scrolling - simplified for speed
            try:
                # Get page height
                page_height = self.driver.execute_script("return document.body.scrollHeight")
                
                # Scroll down once
                scroll_to = random.randint(300, max(400, page_height // 2))
                self.driver.execute_script(f"window.scrollTo(0, {scroll_to})")
                time.sleep(0.5)
                
            except Exception as e:
                self.log_warning(f"Error during scrolling simulation: {str(e)}")
                
        except Exception as e:
            self.log_warning(f"Error in human behavior simulation: {str(e)}")
    
    def parse_product(self, response):
        """Process a product using Selenium WebDriver"""
        # Get product information from meta
        row = response.meta["row"]
        url = row["PriceLink"]
        url_id = row.get("BNCode", "unknown")
        
        if response.status == 404:
            # Create product item with default values for missing product
            item = ProductItem()
            item["price_link"] = row["PriceLink"]
            item["xpath_result"] = "0.00"
            item["out_of_stock"] = "Outstock"
            item["market_player"] = self.market_player
            if "BNCode" in row:
                item["bn_code"] = row["BNCode"]
            
            self.log_info(f"Setting default values for 404 {url_id}: price=0.00, status=Outstock")
            yield item
            
            # Continue with next product if any
            if self.product_queue:
                yield self.get_next_product_request()
            else:
                self.close_webdriver()
            return
        
        self.log_info(f"Processing product: {url_id} - URL: {url}")
        
        # Add human-like delay
        if self.use_human_like_delay:
            sleep_time = random.uniform(self.min_sleep_time, self.max_sleep_time)
            self.log_info(f"😴 Sleeping for {sleep_time:.2f} seconds before processing {url_id}")
            time.sleep(sleep_time)
        
        # Check if WebDriver is initialized
        if not self.driver_initialized and not self.init_webdriver():
            self.log_error(f"Cannot process {url_id} without WebDriver")
            item = self.create_cloudflare_blocked_item(row)
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
        max_attempts = 2
        cloudflare_detected = False
        
        for attempt in range(1, max_attempts + 1):
            try:
                # Load the page with WebDriver
                self.log_info(f"Loading {url} with WebDriver... (Attempt {attempt}/{max_attempts})")
                
                # For each new attempt, we create a fresh session with our headers
                if attempt > 1:
                    # Refresh the browser if we're retrying
                    self.refresh_webdriver()
                
                # Direct navigation to product URL
                self.driver.get(url)
                
                # Short wait
                time.sleep(2)
                
                # Handle consent dialog if present
                if self.handle_consent_dialog():
                    self.log_info("Consent dialog handled successfully")
                    time.sleep(1)
                
                # Simulate some human-like scrolling
                self.simulate_human_behavior()
                
                # Try to wait for page elements
                try:
                    # Wait for a common element on the product page
                    WebDriverWait(self.driver, self.wait_timeout).until(
                        lambda d: any(indicator in d.page_source for indicator in self.success_indicators) or 
                                "produkt niedostępny" in d.page_source.lower()
                    )
                    
                    # Check if we've found product information
                    if any(indicator in self.driver.page_source for indicator in self.success_indicators):
                        self.log_info(f"Page for {url_id} loaded successfully")
                        success = True
                        break
                    
                except TimeoutException:
                    self.log_warning(f"Timeout waiting for page elements for {url_id}")
                    
                    # Check if we have any product information despite the timeout
                    if any(indicator in self.driver.page_source for indicator in self.success_indicators):
                        self.log_info(f"Found some product information despite timeout for {url_id}")
                        success = True
                        break
                
                # Check if this appears to be a CloudFlare block
                if not success:
                    page_source = self.driver.page_source.lower()
                    if any(cf_indicator in page_source for cf_indicator in ['cloudflare', 'cf-challenge', 'verify you are human', 'captcha', 'cf_captcha']):
                        self.log_warning(f"❌ Detected CloudFlare protection on attempt {attempt}")
                        cloudflare_detected = True
            
            except WebDriverException as e:
                self.log_error(f"WebDriver error for {url_id} on attempt {attempt}: {str(e)}")
                
                # If this is the last attempt, break
                if attempt == max_attempts:
                    break
                
                # Otherwise wait a bit before retrying
                time.sleep(2)
        
        # Create a product item based on the result
        if success:
            item = self.extract_product_info(row, self.driver.page_source)
        else:
            if cloudflare_detected:
                self.log_error(f"❌ CloudFlare blocked {url_id} after {max_attempts} attempts - marking for retry")
                item = self.create_cloudflare_blocked_item(row)
            else:
                self.log_error(f"❌ Failed to load page for {url_id} after {max_attempts} attempts")
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

            # Try to get price from JSON-LD data first
            json_ld_elements = tree.xpath("//script[@type='application/ld+json' and contains(text(),'priceCurrency')]/text()")
            if json_ld_elements:
                try:
                    data = json.loads(json_ld_elements[0])
                    price = 'offers' in data and data['offers']['price'] or ""
                    price = self.fix_price(str(price))

                    # Check if availability indicates out of stock
                    if 'offers' in data and 'availability' in data['offers']:
                        availability = data['offers']['availability']
                        if 'outofstock' in availability.lower():
                            stock_status = "Outstock"
                            price = "0.00"
                        else:
                            stock_status = "Instock"
                except Exception as e:
                    self.log_error(f"Error parsing JSON-LD: {str(e)}")
            
            if not price:
                # Try multiple price selectors specific to MediaMarkt
                price_selectors = [
                    "//div[@data-test='cofr-price mms-branded-price']/div/div/span/text()",
                    "//span[@class='price']/text()",
                    "//div[contains(@class, 'price')]/span/text()",
                    "//div[contains(@class, 'price-wrapper')]/div/span/text()",
                    "//meta[@property='product:price:amount']/@content",
                ]
                
                for selector in price_selectors:
                    price_elements = tree.xpath(selector)
                    if price_elements:
                        raw_price = price_elements[0]
                        price = self.fix_price(raw_price)
                        if price:
                            break
            
            # Check stock status if not determined from JSON-LD
            if not stock_status:
                # MediaMarkt specific stock status checks
                in_stock_selectors = [
                    "//button[@id='pdp-add-to-cart-button'][@aria-disabled='false']",
                    "//button[contains(@id, 'add-to-cart')][@disabled='false']",
                    "//button[contains(text(), 'Dodaj do koszyka')]",
                    "//div[contains(@class, 'availability') and contains(text(), 'Dostępny')]",
                ]
                
                out_of_stock_selectors = [
                    "//div[contains(@class, 'availability') and contains(text(), 'Niedostępny')]",
                    "//button[contains(@id, 'add-to-cart')][@disabled='true']",
                    "//div[contains(text(), 'Produkt niedostępny')]",
                    "//button[@id='pdp-add-to-cart-button'][@aria-disabled='true']",
                ]
                
                # Check if any in-stock indicators exist
                if any(self.element_exists(tree, selector) for selector in in_stock_selectors):
                    stock_status = "Instock"
                # Check if any out-of-stock indicators exist
                elif any(self.element_exists(tree, selector) for selector in out_of_stock_selectors):
                    stock_status = "Outstock"
                    price = "0.00"
            else:
                    # Default case if no explicit indicators
                stock_status = "Outstock"
                price = "0.00"
                
            self.log_info(f"{url_id}: {price} PLN | {stock_status}")
            
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
    
    def create_cloudflare_blocked_item(self, row):
        """Create a product item for CloudFlare blocked products to be retried later"""
        url = row["PriceLink"]

        item = ProductItem()
        item["price_link"] = url
        item["xpath_result"] = ""  # Empty price
        item["out_of_stock"] = ""  # Empty stock status to indicate retry needed
        item["market_player"] = self.market_player
        if "BNCode" in row:
            item["bn_code"] = row["BNCode"]
            
        return item
    
    def parse(self, response):
        """Default parse method - delegates to parse_product"""
        row = {"PriceLink": response.url, "MarketPlayer": self.market_player}
        if "row" in response.meta:
            row = response.meta["row"]
            
        return self.parse_product(response)
        
    def __del__(self):
        """Ensure WebDriver is closed when spider is garbage collected"""
        self.close_webdriver() 