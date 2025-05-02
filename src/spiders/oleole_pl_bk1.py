from src.spiders.base_spider import BaseSpider
from src.items import ProductItem
from src.utils.proxy_manager import ProxyManagerFreeProxyListDotNet
from src.utils.price_formatter import format_pl_price
import scrapy
import logging
import json
import random
import time
from urllib.parse import urlparse

class Oleole(BaseSpider):
    name = "oleole"
    market_player = "Oleole"
    use_proxy_manager = True  # Enable using the proxy manager
    use_human_like_delay = True

    # Custom sleep time range for human-like delays
    min_sleep_time = 5  # Minimum sleep time in seconds
    max_sleep_time = 10  # Maximum sleep time in seconds
    between_products_delay = 3  # Additional delay between products when using proxy manager
    between_retries_delay = 1  # Delay before retrying with a new proxy after failure
    if use_proxy_manager:
        min_sleep_time = 1
        max_sleep_time = 2
        between_products_delay = 1
        between_retries_delay = 2
    # Retry settings (for non-proxy manager mode)
    max_retries = 3  # Maximum retries per individual URL
    retry_delay_base = 30  # Base delay in seconds (will be used for exponential backoff)
        
    # custom_settings = {
    #     **BaseSpider.custom_settings,
    #     'HTTPCACHE_ENABLED':0,
    # }
    custom_settings = {
        **BaseSpider.custom_settings,
        'CONCURRENT_REQUESTS':1,
        'DOWNLOAD_DELAY':3,
        'HTTPCACHE_ENABLED':0,
    }

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
        'referer': 'https://oleole.pl/',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
    }

    def __init__(self, input_data=None, *args, **kwargs):
        super().__init__(input_data, *args, **kwargs)
        # Use the Poland-specific proxy manager instead of the default one
        if self.use_proxy_manager:
            self.proxy_manager = ProxyManagerFreeProxyListDotNet()
            self.proxy_manager.logger = self.custom_logger
            # Set minimum proxy count for testing
            self.proxy_manager.min_proxy_count = 2 
        
        # Initialize a dictionary to track retries per URL
        self.url_retries = {}
    
    def element_exists(self, response, xpath):
        """Helper method to check if element exists"""
        return len(response.xpath(xpath)) > 0
        
    def fix_price(self, price_text):
        """Helper method to clean price string"""
        if price_text:
            # Format the price to have 2 decimal places
            return format_pl_price(price_text)
        return ""
        
    def is_validation_page(self, response):
        if 'validate.perfdrive.com' in response.url:
            return True
        if super().is_validation_page(response):
            return True
            
        return False

    def start_requests(self):
        """Override start_requests to ensure proper headers are being used"""
        if not self.market_player:
            raise ValueError(f"market_player must be defined in {self.name}")
        
        self.log_info(f"🕷️ Starting {self.market_player} spider with custom headers")
        
        # Filter rows that match our market player
        product_rows = [row for row in self.input_data if row["MarketPlayer"] == self.market_player]
        
        if not product_rows:
            self.log_info(f"⚠️ No rows found for {self.market_player}")
            return
        
        self.log_info(f"🔍 Total products to scrape: {len(product_rows)}")
        
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
                # Add a random sleep time if human-like delay is enabled
                if self.use_human_like_delay:
                    sleep_time = random.uniform(self.min_sleep_time, self.max_sleep_time)
                    self.log_info(f"😴 Sleeping for {sleep_time:.2f} seconds before request {i+1}/{len(product_rows)}")
                    time.sleep(sleep_time)
                
                # Get headers with a random user agent - this now uses the class's custom headers
                headers = self.get_headers_with_random_ua()
                
                original_url = row["PriceLink"]
                # Initialize retry counter for this URL
                if original_url not in self.url_retries:
                    self.url_retries[original_url] = 0
                
                meta = {
                    "row": row,
                    "product_rows": product_rows,
                    "product_index": i,
                    "original_url": original_url  # Store the original URL for handling redirects
                }
                
                # Automatically use your own proxy even when proxy_manager is disabled
                if not self.use_proxy_manager and hasattr(self, 'proxy_manager') and self.proxy_manager:
                    own_proxy = self.proxy_manager.own_proxy
                    proxy_url = f"http://{own_proxy}"
                    self.log_info(f"Using your own proxy directly: {own_proxy}")
                    meta["proxy"] = proxy_url
                
                yield scrapy.Request(
                    url=original_url,
                    callback=self.parse,
                    headers=headers,
                    meta=meta,
                    dont_filter=True
                )
                
    def parse(self, response):
        row = response.meta["row"]
        product_rows = response.meta.get("product_rows", [])
        product_index = response.meta.get("product_index", 0)
        original_url = response.meta.get("original_url", row["PriceLink"])
        price_link = row["PriceLink"]
        url_id = row.get("BNCode", "unknown")  # For logging only
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
        # Check for proxy manager mode first
        if self.use_proxy_manager:
            proxy_manager = response.meta.get("proxy_manager", self.proxy_manager)
            
            # Check for error responses or validation pages
            if response.status != 200 or self.is_validation_page(response):
                # Use yield from to properly handle the generator from try_next_proxy
                next_request = proxy_manager.try_next_proxy(response)
                if next_request:
                    yield next_request
                    return  # Return without value after yielding
                return
        # If not using proxy manager, handle retries ourselves
        elif self.is_validation_page(response) or (response.status != 200 and 'validate.perfdrive.com' not in response.url):
            # Get current retry count for this URL
            current_retries = self.url_retries.get(original_url, 0)
            
            # Implement retry logic for non-proxy mode
            if current_retries < self.max_retries:
                # Increment retry count for this URL
                current_retries += 1
                self.url_retries[original_url] = current_retries
                
                # Calculate exponential backoff delay
                backoff_delay = self.retry_delay_base * (2 ** (current_retries - 1))
                
                # Add some randomness to avoid detection
                jitter = random.uniform(0.5, 1.5)
                final_delay = backoff_delay * jitter
                
                self.log_warning(f"⚠️ Validation page or error ({response.status}) detected for {url_id}. Retry {current_retries}/{self.max_retries} after {final_delay:.2f} seconds")
                
                # Get fresh headers with a different user agent
                headers = self.get_headers_with_random_ua()
                
                # Create a new meta dictionary
                meta = response.meta.copy()
                
                # Sleep for the calculated delay
                time.sleep(final_delay)
                
                # Yield a new request with the updated meta
                yield scrapy.Request(
                    url=original_url,  # Always use the original URL, not any redirects
                    callback=self.parse,
                    headers=headers,
                    meta=meta,
                    dont_filter=True
                )
                return
            else:
                # We've exceeded max retries, log error and continue with empty values
                self.log_error(f"⚠️ Max retries ({self.max_retries}) exceeded for {url_id}. Unable to bypass validation page.")
                # We'll continue processing with empty values

        try:
            # Initialize price and stock status
            price = ""
            stock_status = ""

            data_text = response.xpath("//script[@type='application/ld+json' and contains(text(),'priceCurrency')]/text()").get()
            if data_text:
                data = json.loads(data_text)
                price = 'offers' in data and data['offers']['price'] or ""
                # Format price to have 2 decimal places using our formatter
                price = format_pl_price(str(price))
            
            if not price:
                # Extract price and stock status based on the provided logic
                try:
                    if self.element_exists(response, "//div[@class='product-card__sidebar product-card__sidebar_tabs-active ng-star-inserted']"):
                        price_element = response.xpath("//div[@class='product-card__sidebar product-card__sidebar_tabs-active ng-star-inserted']/eui-tabs/div/button/span/ems-price/div/div/text()").get()
                        if price_element:
                            price = self.fix_price(price_element)
                    else:
                        price_element = response.xpath("//div[@class='product-card__sidebar ng-star-inserted']/eui-box/div/ems-product-purchase/div/ems-price/div/div/text()").get()
                        if price_element:
                            price = self.fix_price(price_element)
                except Exception as e:
                    pass
            
            if not price:
                price_xpath = "//span[@class='price-template__default--amount']/text()"
                raw_price = self.extract_data(response, price_xpath)
                # Format the raw price
                price = format_pl_price(raw_price)

            # Check stock status based on page elements
            if self.element_exists(response, "//li[@class='price-tab price-outlet-tab is-active']"):
                stock_status = "Outstock"  # Match Excel file's existing value
                price = "0.00"  # Format as 0.00 for consistency
            elif self.element_exists(response, "//button[@class='space__m-t--6 cta--full-width cta']/span[2]"):
                stock_status = "Outstock"  # Match Excel file's existing value
                price = "0.00"  # Format as 0.00 for consistency
            elif self.element_exists(response, "//*[@class='product-card__message']"):
                stock_status = "Outstock"  # Match Excel file's existing value
                price = "0.00"  # Format as 0.00 for consistency
            elif self.element_exists(response, "//*[@class='product-status__item text-alert-20 record record--nested block--has-icon']"):
                stock_status = "Outstock"  # Match Excel file's existing value
                price = "0.00"  # Format as 0.00 for consistency
            elif response.xpath("//button[not(@data-test='add-product-to-the-cart')]"):
                stock_status = "Outstock"  # Match Excel file's existing value
                price = "0.00"  # Format as 0.00 for consistency
                
            else:
                stock_status = "Instock"
                
            # Check if we got any price - if not but the response is 200, it might still be a validation page
            if not price and response.status == 200 and 'validate.perfdrive.com' in response.url:
                self.log_warning(f"Received a 200 response from validate.perfdrive.com but couldn't extract price for {url_id}")
                
                # Check if we should retry
                current_retries = self.url_retries.get(original_url, 0)
                if current_retries < self.max_retries and not self.use_proxy_manager:
                    # This is a validation page with 200 status but no product info - retry
                    current_retries += 1 
                    self.url_retries[original_url] = current_retries
                    
                    backoff_delay = self.retry_delay_base * (2 ** (current_retries - 1))
                    jitter = random.uniform(0.5, 1.5)
                    final_delay = backoff_delay * jitter
                    
                    self.log_warning(f"⚠️ Empty price from perfdrive URL for {url_id}. Retry {current_retries}/{self.max_retries}")
                    
                    # Get fresh headers
                    headers = self.get_headers_with_random_ua()
                    meta = response.meta.copy()
                    
                    time.sleep(final_delay)
                    
                    yield scrapy.Request(
                        url=original_url,
                        callback=self.parse,
                        headers=headers,
                        meta=meta,
                        dont_filter=True
                    )
                    return
                
            # Calculate elapsed time since spider start
            elapsed_time = self.get_elapsed_time()
                
            # Concise logging of just the essential information with timing
            remaining = len(product_rows) - (product_index + 1)
            self.log_info(f"[{product_index+1}/{len(product_rows)}] {url_id}: {price} PLN | {stock_status} | {remaining} remaining | {elapsed_time}")
                
        except Exception as e:
            self.log_error(f"⚠️ Error processing {url_id}: {str(e)}")
            price = ""
            stock_status = ""

        # At the end of parse method, before yielding the item
        item = ProductItem()
        item["price_link"] = price_link
        item["xpath_result"] = price
        item["out_of_stock"] = stock_status
        item["market_player"] = self.market_player
        if "BNCode" in row:
            item["bn_code"] = row["BNCode"]
            
        # Increment the processed items counter
        self.increment_processed_count()
            
        yield item
        
        if self.use_proxy_manager:
            # Handle next product with proxy manager
            next_product_index = product_index + 1
            proxy_manager = response.meta.get("proxy_manager", self.proxy_manager)
            next_request = proxy_manager.process_next_product(response, next_product_index)
            if next_request:
                yield next_request

    