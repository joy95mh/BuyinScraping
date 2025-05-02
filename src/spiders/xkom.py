from src.spiders.base_spider import BaseSpider
from src.items import ProductItem
from src.utils.proxy_manager import ProxyManagerFreeProxyListDotNet
from src.utils.price_formatter import format_pl_price
import scrapy
import logging
import json
import random
import time

class Xkom(BaseSpider):
    name = "xkom"
    market_player = "x-kom"
    use_proxy_manager = True
    use_human_like_delay = True

    min_sleep_time = 2  # Increase minimum sleep time in seconds
    max_sleep_time = 5  # Increase maximum sleep time in seconds
    between_products_delay = 1  # Increase delay between products
    between_retries_delay = 1  # Increase delay before retrying with a new proxy

    custom_settings = {
        **BaseSpider.custom_settings,
        "RANDOMIZE_DOWNLOAD_DELAY": True,  # Randomize delay by 0.5-1.5x
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,vi;q=0.8,th;q=0.7',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-arch': '"x86"',
        'sec-ch-ua-bitness': '"64"',
        'sec-ch-ua-full-version': '"135.0.7049.95"',
        'sec-ch-ua-full-version-list': '"Google Chrome";v="135.0.7049.95", "Not-A.Brand";v="8.0.0.0", "Chromium";v="135.0.7049.95"',
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
        # 'cookie': 'cf_clearance=Bej06v8otjSpwrFVkJ_YxJYTsjyq1M5dqs10fSKbXnA-1744951322-1.2.1.1-0NrBNGvGBxKS.rVpsYDQDqVEbSmDrHUDGNa1Rf_0.qHGJaoeXdxp2pJ5j_C6RUrsoaMiq.TgbDh27HJeJQmhrE0YQFtz4P8xx_N4Sh_09j.QlhzT2aExk57ynaZf6zRrYjWQGc9dPh.8V3XfGvb_knIZfegNKZtE2urc1q2p0CFrCfGHAnPdwvtJBfrq8xF7_EPquTVzBDOoDRs3_9u0O4EG5sehV.CASxQr5K7XPAPySKPGavlEUAZOLaqcFTg4d7pqxfzp_k8501p8wCoHeT0e4TKwpE08NbYupHNUDzdi_EDBSSnZn1StOHcRya2SZlwdVjk8zodbRahny19gOLfTiMKPI4yvoFlkp5HofBgXsSV7j3WC8RQgWI14unPg; ai_user=NnxPft/Rzwip8q1W5D5J9w|2025-04-18T04:42:04.658Z; is_user_logged_in=false; pageThemeMode=dark-system; recently_viewed=[%221280542%22%2C%221173062%22]; ai_session=DDPdFHkvVNW32J/VQIKamb|1744951326848|1744951331094; breakpointName=md',
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
        """x-kom specific validation page detection"""
        # Use the base implementation first
        if super().is_validation_page(response):
            return True
            
        return False

    def parse(self, response):
        # First check for validation pages/errors with parent class
        if response.status != 200 or self.is_validation_page(response):
            proxy_manager = response.meta.get("proxy_manager", self.proxy_manager)
            if self.use_proxy_manager and proxy_manager:
                # Use yield from to properly handle the generator from try_next_proxy
                next_request = proxy_manager.try_next_proxy(response)
                if next_request:
                    yield next_request
                    return  # Return without value after yielding
            else:
                self.log_error(f"⚠️ Error or validation page detected but no proxy manager available")
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
                data = json.loads(data_text)
                price = 'offers' in data and data['offers']['price'] or ""
                price = self.format_price(str(price))

                # Check if availability indicates out of stock
                if 'offers' in data and 'availability' in data['offers']:
                    json_availability = True
                    avail = data['offers']['availability']
                    if 'OutOfStock' in avail:
                        stock_status = "Outstock"
                        price = "0.00"
                    else:
                        stock_status = "Instock"
            
            if not price:
                price_xpath = self.get_config_value("xpath_price", "//div[contains(@class, 'price')]//span[contains(@class, 'whole')]/text()")
                raw_price = self.extract_data(response, price_xpath)
                price = format_pl_price(raw_price)

            if not stock_status:
                # x-kom specific out-of-stock indicators
                if self.element_exists(response, "//div[contains(@class, 'unavailable')]"):
                    stock_status = "Outstock"
                    price = "0.00"
                elif self.element_exists(response, "//span[contains(text(), 'Produkt wycofany')]") or \
                    self.element_exists(response, "//div[contains(text(), 'Produkt został wyprzedany')]"):
                    stock_status = "Outstock"
                    price = "0.00"
                elif not stock_status and not json_availability:
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