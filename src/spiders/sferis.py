from src.spiders.base_spider import BaseSpider
from src.items import ProductItem
from src.utils.proxy_manager import ProxyManagerFreeProxyListDotNet
from src.utils.price_formatter import format_pl_price
import scrapy
import json
import random
import time

class Sferis(BaseSpider):
    name = "sferis"
    market_player = "Sferis"
    use_proxy_manager = False
    use_human_like_delay = True
    # Custom sleep time range for human-like delays
    min_sleep_time = 1  # Minimum sleep time in seconds
    max_sleep_time = 5  # Maximum sleep time in seconds
    between_products_delay = 2  # Additional delay between products when using proxy manager
    between_retries_delay = 1  # Delay before retrying with a new proxy after failure
    
    custom_settings = {
        **BaseSpider.custom_settings,
        'DOWNLOAD_DELAY': 3,
        'HTTPERROR_ALLOW_ALL': False,
        'HTTPERROR_ALLOWED_CODES': [404, 410],
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
        if self.use_proxy_manager:
            proxy_manager = response.meta.get("proxy_manager", self.proxy_manager)
            
            if response.status != 200 or self.is_validation_page(response):
                next_request = proxy_manager.try_next_proxy(response)
                if next_request:
                    yield next_request
                    return
        else:
            # Handle 410 status code - treat as product not available
            if response.status == 410:
                self.log_info(f"🚫 Product gone (410): {response.url}")
                row = response.meta["row"]
                product_rows = response.meta.get("product_rows", [])
                product_index = response.meta.get("product_index", 0)
                price_link = row["PriceLink"]
                url_id = row.get("BNCode", "unknown")
                
                # Return product as out of stock with price 0.00
                item = ProductItem()
                item["price_link"] = price_link
                item["xpath_result"] = "0.00"
                item["out_of_stock"] = "Outstock"
                item["market_player"] = self.market_player
                if "BNCode" in row:
                    item["bn_code"] = row["BNCode"]
                    
                self.log_info(f"[{product_index+1}/{len(product_rows)}] {url_id}: 0.00 PLN | Outstock (Gone 410)")
                yield item
                return
                
            if response.status != 200 or self.is_validation_page(response):
                self.log_error(f"⚠️ Invalid response for {response.url}")
                return
        
        row = response.meta["row"]
        product_rows = response.meta.get("product_rows", [])
        product_index = response.meta.get("product_index", 0)
        price_link = row["PriceLink"]
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
        try:
            price = ""
            stock_status = ""
            raw_price = ""
            
            # Try to get price from JSON-LD data first
            data_text = response.xpath("//script[@type='application/ld+json' and contains(text(),'priceCurrency')]/text()").get()
            if data_text:
                try:
                    data = json.loads(data_text)
                    if 'offers' in data and 'price' in data['offers']:
                        raw_price = data['offers']['price']
                        price = format_pl_price(str(raw_price))

                        # Check if availability indicates out of stock
                        if 'availability' in data['offers']:
                            availability = data['offers']['availability']
                            if 'OutOfStock' in availability:
                                stock_status = "Outstock"
                                price = "0.00"
                            else:
                                # If we have a positive price and availability is not OutOfStock, mark as in stock
                                stock_status = "Instock"
                except Exception as e:
                    self.log_error(f"Error parsing JSON-LD: {str(e)}")
            
            # If no price found, try XPath
            if not price:
                price_xpath = "//div[@class='sl']/text()"
                raw_price = self.extract_data(response, price_xpath)
                if raw_price:
                    price = format_pl_price(raw_price)

            # Check for out-of-stock indicators only if we don't have stock status yet
            if not stock_status:
                # Check for specific out-of-stock text
                if self.element_exists(response, "//span[contains(text(), 'Produkt niedostępny')]"):
                    stock_status = "Outstock"
                    price = "0.00"
                # Only check for button if we don't have a price
                elif not price and self.element_exists(response, "//a[not(contains(@class, 'jsCartAddHref'))]"):
                    stock_status = "Outstock"
                    price = "0.00"
                # If we have a price but no stock status yet, it's likely in stock
                elif price:
                    stock_status = "Instock"
                else:
                    # Default to out of stock if we can't find a price
                    stock_status = "Outstock"
                    price = "0.00"
            
            remaining = len(product_rows) - (product_index + 1)
            self.log_info(f"[{product_index+1}/{len(product_rows)}] {url_id}: {price} PLN | {stock_status} | {remaining} remaining")
                
        except Exception as e:
            self.log_error(f"⚠️ Error processing {url_id}: {str(e)}")
            price = "0.00"
            stock_status = "Outstock"

        item = ProductItem()
        item["price_link"] = price_link
        item["xpath_result"] = price
        item["out_of_stock"] = stock_status
        item["market_player"] = self.market_player
        if "BNCode" in row:
            item["bn_code"] = row["BNCode"]
        yield item
        
        if self.use_proxy_manager:
            next_product_index = product_index + 1
            next_request = proxy_manager.process_next_product(response, next_product_index)
            if next_request:
                yield next_request 