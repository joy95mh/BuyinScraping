from src.spiders.base_spider import BaseSpider
from src.items import ProductItem
from src.utils.proxy_manager import ProxyManagerFreeProxyListDotNet
from src.utils.price_formatter import format_pl_price
import scrapy
import logging
import json
import random
import time
import html

class PlayVariants(BaseSpider):
    name = "play_variants"
    market_players = ["Play S", "Play M", "Play L"]  # Support multiple market players
    use_proxy_manager = False
    use_human_like_delay = True
    min_sleep_time = 1  # Minimum sleep time in seconds
    max_sleep_time = 3  # Maximum sleep time in seconds

    custom_settings = {
        **BaseSpider.custom_settings,
        "RANDOMIZE_DOWNLOAD_DELAY": True,  # Randomize delay by 0.5-1.5x
    }
        
    def __init__(self, input_data=None, market_player=None, input_file=None, *args, **kwargs):
        # Call parent init first to setup logger
        super().__init__(input_data, input_file=input_file, *args, **kwargs)
        
        # Allow explicitly specifying the market player from the crawler, overriding data detection
        if market_player and market_player in self.market_players:
            self.market_player = market_player
            self.log_info(f"Using specified market_player: {self.market_player}")
        # Otherwise, try to detect from input data
        elif input_data and len(input_data) > 0:
            # Get the market player from the first row
            self.market_player = input_data[0].get("MarketPlayer", "")
            self.log_info(f"Detected market_player from data: {self.market_player}")
        else:
            # Default to first one if no data and no explicit market player
            self.market_player = self.market_players[0]
            self.log_info(f"No input data or explicit market_player, defaulting to: {self.market_player}")
    
    def start_requests(self):
        """Override start_requests to handle multiple market players"""
        if not self.market_player:
            raise ValueError(f"market_player must be defined in {self.name}")
        
        self.log_info(f"🕷️ Starting {self.market_player} spider")
        
        # Filter rows that match our specific market player variant
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
                    sleep_time = random.uniform(1, 3)
                    self.log_info(f"😴 Sleeping for {sleep_time:.2f} seconds before request {i+1}/{len(product_rows)}")
                    time.sleep(sleep_time)
                
                # Get headers with a random user agent
                headers = self.get_headers_with_random_ua()
                
                # If we have a proxy manager but it's disabled, use the own_proxy directly
                meta = {
                    "row": row,
                    "product_rows": product_rows,
                    "product_index": i
                }
                
                # Automatically use your own proxy even when proxy_manager is disabled
                if not self.use_proxy_manager and hasattr(self, 'proxy_manager') and self.proxy_manager:
                    own_proxy = self.proxy_manager.own_proxy
                    proxy_url = f"http://{own_proxy}"
                    self.log_info(f"Using your own proxy directly: {own_proxy}")
                    meta["proxy"] = proxy_url
                
                yield scrapy.Request(
                    url=row["PriceLink"],
                    callback=self.parse,
                    headers=headers,
                    meta=meta,
                    dont_filter=True
                )
    
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

    def parse(self, response):
        if self.use_proxy_manager:
            proxy_manager = response.meta.get("proxy_manager", self.proxy_manager)
            
            if response.status != 200 or self.is_validation_page(response):
                next_request = proxy_manager.try_next_proxy(response)
                if next_request:
                    yield next_request
                    return
        else:
            if response.status != 200 or self.is_validation_page(response):
                self.log_error(f"⚠️ Invalid response for {response.url}")
                return
       
        row = response.meta["row"]
        product_rows = response.meta.get("product_rows", [])
        product_index = response.meta.get("product_index", 0)
        price_link = row["PriceLink"]
        url_id = row.get("BNCode", "unknown")
        market_player = row.get("MarketPlayer", self.market_player)
        try:
            price = ""
            stock_status = ""
            json_availability = False
            # Try to get price from JSON-LD data first
            data_text = response.xpath("//script[@type='application/ld+json' and contains(text(),'priceCurrency')]/text()").get()
            if data_text:
                # Clean the JSON data by decoding HTML entities
                data_text = html.unescape(data_text.strip())
                try:
                    data = json.loads(data_text)
                    
                    # Check if availability indicates out of stock
                    if 'offers' in data and 'availability' in data['offers']:
                        json_availability = True
                        if 'OutOfStock' in data['offers']['availability']:
                            stock_status = "Outstock"
                            price = "0.00"
                except json.JSONDecodeError as e:
                    self.log_error(f"⚠️ Error parsing JSON-LD: {str(e)}")
                    self.log_debug(f"Raw JSON-LD data: {data_text[:200]}")
            if not price:
                # Play specific price selectors
                price_element = response.xpath("//span[@class='single-price__current']/text()").get()
                if price_element:
                    price = self.fix_price(price_element)

            if not stock_status:
                # Play specific out-of-stock indicators
                if self.element_exists(response, "//*[@class='unavailable']"):
                    stock_status = "Outstock"
                    price = "0.00"
                elif self.element_exists(response, "//button[contains(text(), 'Produkt niedostępny')]"):
                    stock_status = "Outstock"
                    price = "0.00"
                elif not json_availability and not stock_status:
                    stock_status = "Outstock"
                    price = "0.00"
                else:
                    stock_status = "Instock"
                
            remaining = len(product_rows) - (product_index + 1)
            self.log_info(f"[{product_index+1}/{len(product_rows)}] {market_player} {url_id}: {price} PLN | {stock_status} | {remaining} remaining")
                
        except Exception as e:
            self.log_error(f"⚠️ Error processing {url_id}: {str(e)}")
            price = ""
            stock_status = ""

        item = ProductItem()
        item["price_link"] = price_link
        item["xpath_result"] = price
        item["out_of_stock"] = stock_status
        item["market_player"] = market_player  # Use the specific variant
        if "BNCode" in row:
            item["bn_code"] = row["BNCode"]
        yield item
        
        if self.use_proxy_manager:
            next_product_index = product_index + 1
            next_request = proxy_manager.process_next_product(response, next_product_index)
            if next_request:
                yield next_request 