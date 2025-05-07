from src.spiders.base_spider import BaseSpider
from src.items import ProductItem
from src.utils.proxy_manager import ProxyManagerFreeProxyListDotNet
from src.utils.price_formatter import format_pl_price
import scrapy
import logging
import json
import random
import time

class XiaomiPL(BaseSpider):
    name = "xiaomipl"
    market_player = "XiaomiPL"
    use_proxy_manager = True
    use_human_like_delay = True
    # Custom sleep time range for human-like delays
    min_sleep_time = 1  # Minimum sleep time in seconds
    max_sleep_time = 5  # Maximum sleep time in seconds
    
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
                for request in proxy_manager.try_next_proxy(response):
                    yield request
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

            if not price:
                # XiaomiPL specific price selectors
                try:
                    price_element = response.xpath("//span[@class='price-item price-item--regular']/text()").get()
                    if price_element:
                        price = self.fix_price(price_element)
                except Exception as e:
                    pass

            # XiaomiPL specific out-of-stock indicators
            if self.element_exists(response, "//form[@data-type='add-to-cart-form']//button[@disabled]"):
                stock_status = "Outstock"
                price = "0.00"
            elif self.element_exists(response, "//form[@data-type='add-to-cart-form']//span[contains(text(), 'Produkt niedostępny')]"):
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