from src.spiders.base_spider import BaseSpider
from src.items import ProductItem
from src.utils.proxy_manager import ProxyManagerFreeProxyListDotNet
from src.utils.price_formatter import format_pl_price
import json
import scrapy
import random
import time

class Komputronik(BaseSpider):
    name = "komputronik"
    market_player = "Komputronik"
    use_proxy_manager = False
    use_human_like_delay = True
    
    # Custom sleep time range for human-like delays
    min_sleep_time = 10  # Minimum sleep time in seconds
    max_sleep_time = 20  # Maximum sleep time in seconds
    between_products_delay = 5  # Additional delay between products when using proxy manager
    between_retries_delay = 3  # Delay before retrying with a new proxy after failure
    
    custom_settings = {
        **BaseSpider.custom_settings,
        'DOWNLOAD_DELAY': 5,
        'HTTPERROR_ALLOW_ALL': False,
        'HTTPERROR_ALLOWED_CODES': [404, 410, 429, 503],  # Allow 429 status code
        'DOWNLOAD_TIMEOUT': 30,  # Increase timeout
    }
        
    def __init__(self, input_data=None, *args, **kwargs):
        super().__init__(input_data, *args, **kwargs)
    
    def is_validation_page(self, response):
        """Komputronik specific validation page detection"""
        if 'validate.perfdrive.com' in response.url:
            return True
        # Check for Cloudflare challenges
        if '__cf_chl_captcha_tk__' in response.text or 'cf-browser-verification' in response.text:
            self.log_warning(f"CloudFlare protection detected at URL: {response.url}")
            return True
        # Check for other common rate limit indicators
        if 'rate limit' in response.text.lower() or 'too many requests' in response.text.lower():
            self.log_warning(f"Rate limit text detected in response for URL: {response.url}")
            return True
        # Use the base implementation first
        if super().is_validation_page(response):
            return True
            
        # Komputronik specific validation checks can be added here
        return False

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
                
                # self.log_info(f"🔍 Preparing request {i+1}/{len(product_rows)} | ID: {url_id}")
                
                # Add a random sleep time if human-like delay is enabled - use longer delays for Komputronik
                if self.use_human_like_delay:
                    sleep_time = random.uniform(self.min_sleep_time, self.max_sleep_time)
                    self.log_info(f"😴 Sleeping for {sleep_time:.2f} seconds before request {i+1}/{len(product_rows)}")
                    time.sleep(sleep_time)
                
                # Get headers with a random user agent
                headers = self.get_headers_with_random_ua()
                
                # If we have a proxy manager but it's disabled, use the own_proxy directly
                meta = {
                    "row": row,
                    "product_rows": product_rows,
                    "product_index": i,
                    "handle_httpstatus_list": [404, 410, 429, 503],  # Add 429 to handled status codes
                    "retry_count": 0,  # Track retries
                    "url_id": url_id
                }
                
                self.log_info(f"🚀 Sending request {i+1}/{len(product_rows)} to {url} for {url_id}")
                
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    headers=headers,
                    meta=meta,
                    dont_filter=True
                )

    def parse(self, response):
        row = response.meta["row"]
        product_rows = response.meta.get("product_rows", [])
        product_index = response.meta.get("product_index", 0)
        price_link = row["PriceLink"]
        url_id = response.meta.get("url_id", row.get("BNCode", "unknown"))
        retry_count = response.meta.get("retry_count", 0)
        
        # Log the response status
        self.log_info(f"📋 Response for {url_id} | Status: {response.status} | Size: {len(response.body)} bytes")
        
        # Handle 429 Too Many Requests - implement exponential backoff
        if response.status == 429:
            # Max retries to prevent infinite loops
            if retry_count >= 3:
                
                return
                
            # Calculate exponential backoff delay - the more retries, the longer the wait
            backoff_time = 60 * (2 ** retry_count)  # 60s, 120s, 240s
            self.log_warning(f"⚠️ Rate limited (429) for {url_id}. Retry {retry_count+1}/3 after {backoff_time} seconds")
            
            # Schedule a retry with increased delay
            meta = response.meta.copy()
            meta["retry_count"] = retry_count + 1
            
            # Get fresh headers with a new random user agent
            headers = self.get_headers_with_random_ua()
            
            # Use time.sleep for the backoff (or you could use twisted's reactor.callLater in production)
            time.sleep(backoff_time)
            
            yield scrapy.Request(
                url=response.url,
                callback=self.parse,
                headers=headers,
                meta=meta,
                dont_filter=True
            )
            return
        
        # For other error responses or validation pages
        if response.status != 200 or self.is_validation_page(response):
            proxy_manager = response.meta.get("proxy_manager", self.proxy_manager)
            if self.use_proxy_manager and proxy_manager:
                # Use yield from to properly handle the generator from try_next_proxy
                for request in proxy_manager.try_next_proxy(response):
                    yield request
                return  # Important: return after yielding from generator
            else:
                self.log_error(f"⚠️ Error or validation page detected but no proxy manager available: Status {response.status}")
                return

        try:
            price = ""
            stock_status = ""

            # Try to get price from JSON-LD data first
            data_text = response.xpath("//script[@type='application/ld+json' and contains(text(),'priceCurrency')]/text()").get()
            if data_text:
                data = json.loads(data_text)
                price = 'offers' in data and data['offers']['price'] or ""
                price = self.format_price(str(price))
            
            if not price:
                # Komputronik specific price selectors
                try:
                    price_element = response.xpath("//div[@data-price-type='final']/text()").get()
                    price = self.format_price(price_element)
                except Exception as e:
                    pass

            # Komputronik specific out-of-stock indicators
            if self.element_exists(response, "//button[@data-name='addToCartButton'][@disabled]"):
                stock_status = "Outstock"
                price = "0.00"
            else:
                stock_status = "Instock"
                
            remaining = len(product_rows) - (product_index + 1)
            self.log_info(f"[{product_index+1}/{len(product_rows)}] {url_id}: {price} PLN | {stock_status} | {remaining} remaining")
                
        except Exception as e:
            self.log_error(f"⚠️ Error processing {url_id}: {str(e)}")

        item = ProductItem()
        item["price_link"] = price_link
        item["xpath_result"] = price
        item["out_of_stock"] = stock_status
        item["market_player"] = self.market_player
        if "BNCode" in row:
            item["bn_code"] = row["BNCode"]
        yield item
        
        # Process next product if proxy manager is enabled
        if self.use_proxy_manager:
            proxy_manager = response.meta.get("proxy_manager", self.proxy_manager)
            next_product_index = product_index + 1
            next_request = proxy_manager.process_next_product(response, next_product_index)
            if next_request:
                yield next_request