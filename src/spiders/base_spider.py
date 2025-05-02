import scrapy
import logging
import random
import time, re
from src.utils.config_loader import load_domain_config
from src.utils.price_formatter import format_pl_price
from src.utils.proxy_manager import ProxyManagerFreeProxyListDotNet
from src.utils.logger_setup import setup_spider_logger, log_final_stats
from scrapy.utils.project import get_project_settings
from datetime import datetime, timedelta
from fake_useragent import UserAgent

class BaseSpider(scrapy.Spider):
    custom_settings = {
        "ITEM_PIPELINES": {
            "src.pipelines.output_pipeline.OutputPipeline": 110
        },
        "DUPEFILTER_CLASS": "scrapy.dupefilters.BaseDupeFilter",  # Disable duplicate filtering
        "RETRY_ENABLED": False, 
        "DOWNLOAD_TIMEOUT": 30,  
        'CONCURRENT_REQUESTS': 1, 
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,  
        'DOWNLOAD_DELAY': 1,
        'COOKIES_ENABLED':0,
        'LOG_LEVEL': 'INFO',  # Set default log level
        'LOG_ENABLED': False,  # Disable Scrapy logging completely
        'HTTPCACHE_ENABLED':0,
        'HTTPERROR_ALLOWED_CODES': [202, 404, 410, 429, 503],  # Common error codes to handle
        'HTTPERROR_ALLOW_ALL': False,
    }

    # List of user agents to rotate through
    ua = UserAgent()

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
    }

    market_player = None
    use_proxy_manager = False  # Set to True in a spider to use the proxy manager
    use_human_like_delay = True  # Set to False to disable random delays
    
    # Default sleep time settings that can be overridden by child spiders
    min_sleep_time = 1  # Default minimum sleep time in seconds
    max_sleep_time = 4  # Default maximum sleep time in seconds
    between_products_delay = 1  # Default delay between products when using proxy
    between_retries_delay = 2  # Default delay before retrying with a new proxy

    def __init__(self, input_data=None, input_file=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Initialize the custom file logger
        self.custom_logger = setup_spider_logger(self.name)
        self.input_data = input_data or []
        self.input_file = input_file
        self.config = load_domain_config(self.name)
        
        # Initialize proxy manager in __init__ so we can use our own proxy by default
        self.proxy_manager = None
        if self.use_proxy_manager:
            self.proxy_manager = ProxyManagerFreeProxyListDotNet()
            self.proxy_manager.logger = self.custom_logger
        
        self.settings = get_project_settings()
        self.log_info(f"🕷️ Initializing {self.name} spider")
        self.log_info(f"🔄 Proxy Manager: {'Enabled' if self.use_proxy_manager else 'Disabled'}")
        self.log_info(f"🕒 Human-like delays: {'Enabled' if self.use_human_like_delay else 'Disabled'}")
        
        # Setup test mode if enabled
        self.test_mode = kwargs.get('test_mode', False)
        if self.test_mode:
            self.log_info("🧪 Running in TEST mode - limited products will be processed")
        
        # Initialize timing metrics
        self.start_time = time.time()
        self.end_time = None
        self.processed_items_count = 0
        
        # Set up allowed HTTP error codes
        self.allowed_error_codes = self.custom_settings.get('HTTPERROR_ALLOWED_CODES', [])
        
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        # Check for proxy manager in settings
        if spider.use_proxy_manager and hasattr(crawler.settings, 'get') and crawler.settings.get('PROXY_MANAGER', None):
            spider.proxy_manager = crawler.settings.get('PROXY_MANAGER')
        return spider
        
    def closed(self, reason):
        """Called when the spider is closed"""
        # Log final statistics
        log_final_stats(
            self.custom_logger, 
            self.name, 
            self.processed_items_count,
            self.start_time
        )

    def format_str(self,str):
        str =  str or None
        if str is None: return None
        str = re.sub(r'([\t\n\r])',' ',str)
        str = re.sub(r'([\s]+)',' ',str)
        str = re.sub('^[,\s]|[,\s]$','',str)   
        str = re.sub(r'\xa0|&nbsp;',' ',str)
        return str.encode().decode('utf8').strip()      
    
    def remove_html_tags(self,text):
        text =  text or None
        if text is None: return None
        clean_text = re.sub(r'<[^>]*>', '', text)
        return clean_text
    
    def extract_data(self, response, xpath=None):
        elements = response.xpath(xpath)
        if len(elements) > 1:
            elements = elements[0].getall()
        else:
            elements = elements.getall()
        # Clean up non-breaking spaces and other special characters
        result = self.format_str(self.remove_html_tags(''.join(elements)))
        
        return result
    
    def format_price(self, price_text):
        """Format price to consistent decimal format with 2 digits after decimal point"""
        return format_pl_price(price_text)

    # Override log methods to use our custom logger
    def log(self, message, level=logging.DEBUG, **kw):
        self.custom_logger.log(level, message, **kw)

    # Helper methods for different log levels
    def log_debug(self, message, **kw):
        self.custom_logger.debug(message, **kw)

    def log_info(self, message, **kw):
        self.custom_logger.info(message, **kw)

    def log_warning(self, message, **kw):
        self.custom_logger.warning(message, **kw)

    def log_error(self, message, **kw):
        self.custom_logger.error(message, **kw)
        
    def element_exists(self, response, xpath):
        """Check if an element exists in the response"""
        return len(response.xpath(xpath)) > 0
        
    def is_validation_page(self, response):
        """
        Check if the response is a validation page or an error page.
        Returns True if it detects a validation page, False otherwise.
        """
        # Skip HTTP error code check if it's an allowed error
        if hasattr(response, 'status') and response.status in self.allowed_error_codes:
            self.log_info(f"Received allowed HTTP error: {response.status}")
            return False
            
        # Log response info for debugging
        page_size = len(response.body)
        self.log_debug(f"Response size: {page_size} bytes | URL: {response.url}")
        
        # Too short responses are suspicious (but not always validation pages)
        if page_size < 1000:
            short_content = response.body.decode('utf-8', errors='ignore')[:200]
            self.log_debug(f"Page too short ({page_size} bytes). First 200 chars: {short_content}")
            return True
        
        # Check for HTTP headers dump in the response (common in anti-bot responses)
        headers_dump = any(header in response.text.lower() for header in [
            'user-agent:', 'accept-encoding:', 'accept-language:', 'referer:'
        ])
        if headers_dump:
            self.log_debug("Found HTTP headers dump in the response - likely validation page")
            return True
        
        # Check if the page contains basic HTML structure
        if not ('<html' in response.text.lower() and '<body' in response.text.lower()):
            self.log_debug("Not HTML page")
            # print(response.text)
            return True
            
        return False
    
    def get_random_ua(self):
        """Get a random user agent from the list - handles both simple and structured UA lists"""
        # Filter out entries that aren't actual user agents (e.g., browser version, OS, etc.)
        return self.ua.random

    def get_headers_with_random_ua(self):
        """Get a copy of headers with a random user agent"""
        # Use child class headers if available, otherwise use base class headers
        headers = self.headers.copy() if hasattr(self, 'headers') else self.__class__.headers.copy()
        headers['user-agent'] = self.ua.random
        return headers
    
    def start_requests(self):
        
        """Unified start_requests method that handles both proxy and non-proxy modes with human-like behavior"""
        if not self.market_player:
            raise ValueError(f"market_player must be defined in {self.name}")
        
        self.log_info(f"🕷️ Starting {self.market_player} spider")
        
        # Filter rows that match our market player
        product_rows = [row for row in self.input_data if row["MarketPlayer"] == self.market_player]
        
        if not product_rows:
            self.log_info(f"⚠️ No rows found for {self.market_player}")
            return
            
        self.log_info(f"🔍 Total products to scrape: {len(product_rows)}")
        
        # Log delay settings for this spider
        if self.use_human_like_delay:
            self.log_info(f"⏱️ Using sleep time range: {self.min_sleep_time}-{self.max_sleep_time} seconds")
            if self.use_proxy_manager:
                self.log_info(f"⏱️ Between products delay: {self.between_products_delay} seconds")
                self.log_info(f"⏱️ Between retries delay: {self.between_retries_delay} seconds")
        
        # If proxy manager is enabled, use it
        if self.use_proxy_manager and self.proxy_manager:
            self.log_info(f"Using proxy manager with your own proxy first")
            yield self.proxy_manager.start_proxy_fetch(
                callback=self.parse,
                spider_instance=self,
                product_rows=product_rows
            )
        else:
            # Direct requests without proxy manager
            self.log_info(f"Using direct requests without proxy manager")
            for i, row in enumerate(product_rows):
                url = row["PriceLink"]
                url_id = row.get("BNCode", "unknown")
                
                # Add a random sleep time if human-like delay is enabled
                if self.use_human_like_delay:
                    sleep_time = random.uniform(self.min_sleep_time, self.max_sleep_time)
                    self.log_info(f"😴 Sleeping for {sleep_time:.2f} seconds before request {i+1}/{len(product_rows)}")
                    time.sleep(sleep_time)
                
                # Get headers with a random user agent
                headers = self.get_headers_with_random_ua()
                
                # Prepare meta dictionary
                meta = {
                    "row": row,
                    "product_rows": product_rows,
                    "product_index": i,
                    "url_id": url_id
                }
                
                # Automatically use your own proxy even when proxy_manager is disabled
                # if not self.use_proxy_manager and hasattr(self, 'proxy_manager') and self.proxy_manager:
                #     own_proxy = self.proxy_manager.own_proxy
                #     proxy_url = f"http://{own_proxy}"
                #     self.log_info(f"Using your own proxy directly: {own_proxy}")
                #     meta["proxy"] = proxy_url
                
                self.log_info(f"🚀 Sending request {i+1}/{len(product_rows)} for {url_id}")
                
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    headers=headers,
                    meta=meta,
                    dont_filter=True
                )

    def parse(self, response):
        """Default parse method - to be implemented by child classes"""
        self.log_error("parse method not implemented in child class")
        raise NotImplementedError("You must implement the parse method in your spider.")

    def increment_processed_count(self):
        """Increment the counter for processed items"""
        self.processed_items_count += 1
        
    def get_elapsed_time(self):
        """Get the elapsed time since spider start"""
        current_time = time.time()
        elapsed_seconds = current_time - self.start_time
        return timedelta(seconds=elapsed_seconds)
        
    def finish_stats(self):
        """Calculate and log final statistics when spider finishes"""
        self.end_time = time.time()
        total_duration = self.end_time - self.start_time
        
        # Format duration nicely
        duration_td = timedelta(seconds=total_duration)
        hours, remainder = divmod(duration_td.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        # Add milliseconds
        duration_str = f"{hours:02}:{minutes:02}:{seconds:02}"
        
        # Calculate items per minute if we have items
        items_per_minute = 0
        if total_duration > 0 and self.processed_items_count > 0:
            items_per_minute = (self.processed_items_count * 60) / total_duration
            
        self.log_info(f"✅ Spider {self.name} completed")
        self.log_info(f"🕒 Total time: {duration_str}")
        self.log_info(f"📊 Processed {self.processed_items_count} items")
        self.log_info(f"⚡ Performance: {items_per_minute:.2f} items/minute")
        
        return {
            "spider_name": self.name,
            "market_player": self.market_player,
            "total_duration_seconds": total_duration,
            "total_duration_formatted": duration_str,
            "processed_items": self.processed_items_count,
            "items_per_minute": items_per_minute
        }
    
    