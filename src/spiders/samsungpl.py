from src.spiders.base_spider import BaseSpider
from src.items import ProductItem
from src.utils.proxy_manager import ProxyManagerFreeProxyListDotNet
from src.utils.price_formatter import format_pl_price
import scrapy
import logging
import json
import random
import time

class SamsungPL(BaseSpider):
    name = "samsungpl"
    market_player = "SamsungPL"
    use_proxy_manager = True
    use_human_like_delay = True
    # Custom sleep time range for human-like delays
    min_sleep_time = 5  # Minimum sleep time in seconds
    max_sleep_time = 10  # Maximum sleep time in seconds
    between_products_delay = 1  # Additional delay between products
    between_retries_delay = 1  # Increase delay before retrying with a new proxy

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
            json_product = response.xpath("//script[@data-object-type='Product']/text()").get()
            json_product_group = response.xpath("//script[@data-object-type='ProductGroup']/text()").get()
            if json_product:
                data = json.loads(json_product)
                price = 'offers' in data and data['offers']['price'] or ""
                price = format_pl_price(str(price))

                # Check if availability indicates out of stock
                if 'offers' in data and 'availability' in data['offers']:
                    json_availability = True
                    if 'outofstock' in data['offers']['availability'].lower():
                        stock_status = "Outstock"
                        price = "0.00"
                    else:
                        stock_status = "Instock"
            elif json_product_group:
                data = json.loads(json_product_group)
                model_code = response.xpath('//script').re_first('modelCode : "([^\"]+)')
                stock_status = "Outstock"
                price = "0.00"
                if model_code:
                    for sel in data[0]['hasVariant']:
                        if sel['sku'] == model_code:
                            price = 'offers' in sel and sel['offers']['price'] or ""
                            price = format_pl_price(str(price))
                            if 'offers' in sel and 'availability' in sel['offers']:
                                json_availability = True
                                if 'outofstock' in sel['offers']['availability'].lower():
                                    stock_status = "Outstock"
                                    price = "0.00"
                                else:
                                    stock_status = "Instock"

            else:
                stock_status = "Outstock"
                price = "0.00"

            # SamsungPL specific out-of-stock indicators
            if self.element_exists(response, "//div[contains(@class, 'out-of-stock')]"):
                stock_status = "Outstock"
                price = "0.00"
            elif self.element_exists(response, "//*[contains(text(), 'Produkt niedostępny')]") or \
                 self.element_exists(response, "//*[contains(text(), 'Produkt tymczasowo niedostępny')]"):
                stock_status = "Outstock"
                price = "0.00"
            elif not stock_status and not json_availability:
                stock_status = "Outstock"
                price = "0.00"
                
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