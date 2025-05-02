from src.spiders.base_spider import BaseSpider
from src.items import ProductItem
from src.utils.proxy_manager import ProxyManagerFreeProxyListDotNet
from src.utils.price_formatter import format_pl_price
import scrapy
import logging
import json
import random
import time

class Zadowolenie(BaseSpider):
    name = "zadowolenie"
    market_player = "Zadowolenie"
    use_proxy_manager = False
    use_human_like_delay = True
    
    # Custom sleep time range for human-like delays
    min_sleep_time = 40  # Minimum sleep time in seconds
    max_sleep_time = 60  # Maximum sleep time in seconds
    between_products_delay = 3  # Additional delay between products when using proxy manager
    between_retries_delay = 1  # Delay before retrying with a new proxy after failure
    
    custom_settings = {
        **BaseSpider.custom_settings,
        'DOWNLOAD_DELAY': 5,
        'HTTPERROR_ALLOW_ALL': False,
        'HTTPERROR_ALLOWED_CODES': [404, 410, 429, 503],  # Allow 429 status code
        'DOWNLOAD_TIMEOUT': 15,  # Increase timeout
    }
        
    def __init__(self, input_data=None, *args, **kwargs):
        super().__init__(input_data, *args, **kwargs)
    
    def element_exists(self, response, xpath):
        return len(response.xpath(xpath)) > 0
        
    def fix_price(self, price_text):
        if price_text:
            return format_pl_price(price_text)
        return ""
        
    def is_validation_page(self, response):
        if super().is_validation_page(response):
            return True
            
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
                self.log_error(f"⚠️ Max retries exceeded for {url_id} after 429 status code")
                # Return product as out of stock when we can't process it
                item = ProductItem()
                item["price_link"] = price_link
                item["xpath_result"] = "0.00"
                item["out_of_stock"] = "Outstock"
                item["market_player"] = self.market_player
                if "BNCode" in row:
                    item["bn_code"] = row["BNCode"]
                yield item
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
                price = data['offers']['price']
                price = format_pl_price(str(price))

                # Check if availability indicates out of stock
                if 'offers' in data and 'availability' in data['offers']:
                    if 'OutOfStock' in data['offers']['availability']:
                        stock_status = "Outstock"
                        price = "0.00"
            
            if not price:
                # Zadowolenie specific price selectors
                try:
                    price_element = response.xpath("//div[@data-type='product-price']/text()").get()
                    if price_element:
                        price = self.fix_price(price_element)
                except Exception as e:
                    pass
            
            if not stock_status:
                # Zadowolenie specific out-of-stock indicators
                if self.element_exists(response, "//script[@type='application/ld+json' and contains(text(),'priceCurrency') and contains(text(), 'OutOfStock')]"):
                    stock_status = "Outstock"
                    price = "0.00"
                elif self.element_exists(response, "//span[contains(text(), 'Produkt niedostępny')]"):
                    stock_status = "Outstock"
                    price = "0.00"
                else:
                    stock_status = "Instock"
                
            remaining = len(product_rows) - (product_index + 1)
            self.log_info(f"[{product_index+1}/{len(product_rows)}] {url_id}: {price} PLN | {stock_status} | {remaining} remaining")
                
        except Exception as e:
            self.log_error(f"⚠️ Error processing {url_id}: {str(e)}")
            price = ""
            stock_status = ""
        
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