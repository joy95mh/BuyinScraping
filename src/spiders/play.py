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

class Play(BaseSpider):
    name = "play"
    market_player = "Play"
    use_proxy_manager = False
    use_human_like_delay = True
    min_sleep_time = 1  # Minimum sleep time in seconds
    max_sleep_time = 3  # Maximum sleep time in seconds
    between_retries_delay = 2  # Override default - lower from 5 to 2 seconds

    custom_settings = {
        **BaseSpider.custom_settings,
        "RANDOMIZE_DOWNLOAD_DELAY": True,  # Randomize delay by 0.5-1.5x
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
                    price = 'offers' in data and data['offers']['price'] or ""
                    price = format_pl_price(str(price))
                    
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
        
        if self.use_proxy_manager:
            next_product_index = product_index + 1
            next_request = proxy_manager.process_next_product(response, next_product_index)
            if next_request:
                yield next_request 