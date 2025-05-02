from src.spiders.base_spider import BaseSpider
from src.items import ProductItem
from src.utils.price_formatter import format_pl_price
import scrapy
import logging
import json
import random
import time
import requests
from lxml import html
import re

class RtvEuroAgd(BaseSpider):
    name = "rtveuroagd"
    market_player = "RTV Euro AGD"
    use_proxy_manager = False
    use_human_like_delay = True
    
    # Delay settings - increased for better success rate
    min_sleep_time = 3  # Minimum sleep time in seconds
    max_sleep_time = 6  # Maximum sleep time in seconds
    between_products_delay = 2  # Additional delay between products

    custom_settings = {
        **BaseSpider.custom_settings,
        "DOWNLOAD_DELAY": 5,
        "DOWNLOAD_TIMEOUT": 60,
        "RETRY_TIMES": 5,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 522, 524, 408, 429, 403],
        "HTTPERROR_ALLOWED_CODES": [403, 404, 429, 503],
        "REDIRECT_ENABLED": False,  # Disable Scrapy's built-in redirect handling
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,vi;q=0.8,th;q=0.7',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
        # 'cookie': 'navTestAB=B; searchTestAB=S; uDashIn=; uDashOut=; uDashUserID=1744817195530/227891798; _pfbuid=047aec67-f1b8-f2fd-4972-5fcb4e55caec; OptanonAlertBoxClosed=2025-04-16T15:26:48.939Z; akaalb_POC-euro-com=~op=www_domain:web_atman|~rv=9~m=web_atman:0|~os=5718243972d2a976aab1286657d190b2~id=e8768d36fa6737c1d16aca1da7c1d6a9; euroLogged=false; _prefixboxIntegrationVariant=; _pfbx_application_information=50c1a6cd-af2c-44c6-8b8a-8e8238775eda; ak_bmsc=46BD0F147F9CC4020E26F5C35B462CF7~000000000000000000000000000000~YAAQHYtIFydCpiKWAQAAdMIdVhvhyDgF+iXX5wkWbna8IWVdqdlfc7okV2aEcCrOK3xR06BdtliEiZTSeLz/zTKBRLTVyslfFmC18sw20V8i36c5kl+KdU25b7xXEgAZ8UQ1F0TnjQ4pMgM8LhtfEW4wnLcN2sp3yUqNjFSEIvIAYsaXLxcQOTT2Wn6x2rey4iPP1em0ucHbx7g9v8wbT0CaRmhqOGcGK+KCWsPbalRRkKEhoW3XRaIxnCZe15N0rjVdkPVv41s7KP9fAmBtCpMTAwBf0jK4hh4K6EpJjyRwBeVep9pY7hWiijp6hnJ6JADtCIZO9J3qMQgAVPjexYJ2lnP5T0xKU5LszM+S7OCffK5rHDc9325fkwwHOTccHZkr1M2b5JoAhGsr; __Host-JSESSIONID=2D25615BDD58EA653DCA85B3C3E40A37.inst4ap83; euroCustomer=7065349741::5; uDashUserVisit=10; OptanonConsent=isGpcEnabled=0&datestamp=Mon+Apr+21+2025+10%3A05%3A42+GMT%2B0700+(Indochina+Time)&version=202502.1.0&browserGpcFlag=0&isIABGlobal=false&consentId=4c1ccc32-f258-4b8b-bee6-192725954b81&interactionCount=2&isAnonUser=1&landingPath=NotLandingPage&AwaitingReconsent=false&groups=C0001%3A1%2CC0003%3A0%2CC0004%3A0%2CC0002%3A0&hosts=H9%3A1%2CH189%3A1%2CH124%3A1%2CH145%3A1%2CH147%3A1%2CH169%3A1%2CH150%3A1%2CH34%3A1%2CH36%3A1%2CH175%3A0%2CH156%3A0%2CH186%3A0%2CH178%3A0%2CH187%3A0%2CH185%3A0%2CH158%3A0%2CH6%3A0%2CH188%3A0%2CH159%3A0%2CH161%3A0%2CH181%3A0%2CH163%3A0%2CH203%3A0%2CH105%3A0%2CH12%3A0%2CH49%3A0%2CH112%3A0%2CH212%3A0%2CH191%3A0%2CH192%3A0%2CH193%3A0%2CH21%3A0%2CH194%3A0%2CH23%3A0%2CH167%3A0%2CH202%3A0%2CH122%3A0%2CH199%3A0%2CH170%3A0%2CH95%3A0%2CH151%3A0%2CH198%3A0%2CH152%3A0%2CH32%3A0%2CH153%3A0%2CH35%3A0%2CH155%3A0%2CH179%3A0%2CH204%3A0%2CH62%3A0%2CH206%3A0&genVendors=&intType=3&geolocation=SE%3BAB',
    }
        
    def __init__(self, input_data=None, *args, **kwargs):
        super().__init__(input_data, *args, **kwargs)
        
        # Create a requests session for our actual HTTP requests
        self.session = self.create_requests_session()
        
        # Store product results
        self.processed_products = []
        
    def create_requests_session(self):
        """Create and configure a requests session with appropriate headers"""
        session = requests.Session()
        headers = self.headers.copy()
        headers['user-agent'] = self.get_random_ua()
        session.headers.update(headers)
        return session
    
    def rotate_user_agent(self):
        """Rotate the user agent in the session"""
        self.session.headers.update({'user-agent': self.get_random_ua()})
    
    def element_exists(self, tree, xpath):
        """Check if an element exists in an lxml HTML tree"""
        return len(tree.xpath(xpath)) > 0
    
    def is_validation_page(self, html_content, status_code):
        """Check if the response is a validation page"""
        # Check status code
        if status_code != 200:
            return True
            
        # Check page size (too small responses are suspicious)
        if len(html_content) < 1000:
            return True
                
        # Check if the page lacks proper HTML structure
        if not ('<html' in html_content.lower() and '<body' in html_content.lower()):
            return True
            
        return False

    def start_requests(self):
        """Start the scraping process directly with product scraping using your VPN connection"""
        self.log_info(f"🕷️ Starting {self.market_player} spider with requests-based fetching using your VPN")
        
        # Filter rows that match our market player
        product_rows = [row for row in self.input_data if row["MarketPlayer"] == self.market_player]
        
        if not product_rows:
            self.log_info(f"⚠️ No rows found for {self.market_player}")
            return
            
        self.log_info(f"🔍 Total products to scrape: {len(product_rows)}")
        
        # Process the first product
        if product_rows:
            # Convert to dummy request to start the process
            meta = {
                "product_rows": product_rows,
                "product_index": 0
            }
            
            # Use a dummy URL that just serves as a placeholder
            yield scrapy.Request(
                url="https://example.com/dummy",
                callback=self.process_product,
                meta=meta,
                dont_filter=True
            )
    
    def process_product(self, response):
        """Process a product using requests instead of scrapy"""
        # Get product information from meta
        product_rows = response.meta["product_rows"]
        product_index = response.meta["product_index"]
        
        if product_index >= len(product_rows):
            # We've processed all products, yield results
            self.log_info(f"✅ Finished processing all {len(product_rows)} products")
            for result in self.processed_products:
                yield result
            return
        
        # Get the current product
        row = product_rows[product_index]
        url = row["PriceLink"]
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
        # Add human-like delay
        if self.use_human_like_delay:
            sleep_time = random.uniform(self.min_sleep_time, self.max_sleep_time)
            self.log_info(f"😴 Sleeping for {sleep_time:.2f} seconds before request {product_index+1}/{len(product_rows)}")
            time.sleep(sleep_time)
        
        # Rotate user agent
        self.rotate_user_agent()
        
        # Make the request using requests
        self.log_info(f"🚀 Sending request {product_index+1}/{len(product_rows)} to {url} for {url_id}")
        
        retry_count = 0
        max_retries = 3
        success = False
        
        while retry_count < max_retries and not success:
            try:
                response = self.session.get(
                    url, 
                    timeout=30,
                )
                
                if self.is_validation_page(response.text, response.status_code):
                    self.log_warning(f"⚠️ Validation page detected for {url_id} (Status: {response.status_code})")
                    # Rotate user agent and retry
                    self.rotate_user_agent()
                    retry_count += 1
                    time.sleep(self.between_products_delay)
                else:
                    # Success!
                    success = True
                    
            except requests.RequestException as e:
                self.log_error(f"⚠️ Request error for {url_id}: {str(e)}")
                # Rotate user agent and retry
                self.rotate_user_agent()
                retry_count += 1
                time.sleep(self.between_products_delay)
        
        # Process the response if successful
        if success:
            try:
                # Parse the HTML
                tree = html.fromstring(response.text)
                
                # Extract price and stock information
                price = ""
                stock_status = ""
                
                # Try to get price from JSON-LD data first
                json_ld_elements = tree.xpath("//script[@type='application/ld+json' and contains(text(),'priceCurrency')]/text()")
                if json_ld_elements:
                    try:
                        data = json.loads(json_ld_elements[0])
                        price = 'offers' in data and data['offers']['price'] or ""
                        price = self.format_pl_price(str(price))

                        # Check if availability indicates out of stock
                        if 'offers' in data and 'availability' in data['offers']:
                            if 'OutOfStock' in data['offers']['availability']:
                                stock_status = "Outstock"
                                price = "0.00"

                    except Exception as e:
                        self.log_error(f"Error parsing JSON-LD: {str(e)}")
                
                if not price:
                    # Multiple possible price selectors
                    price_selectors = [
                        "//div[contains(@class, 'product-price')]//span[@class='price']/text()",
                        "//div[contains(@class, 'product-price')]//span[contains(@class, 'value')]/text()",
                        "//div[contains(@class, 'price-normal')]/text()",
                        "//span[contains(@class, 'product-price')]/text()"
                    ]
                    
                    for selector in price_selectors:
                        elements = tree.xpath(selector)
                        if elements:
                            price_element = elements[0]
                            price = self.format_pl_price(price_element)
                            if price:
                                break
                
                if not stock_status:
                    # RTV Euro AGD specific out-of-stock indicators
                    out_of_stock_selectors = [
                        "//span[@class='product-card__message-text']",
                        "//span[contains(text(), 'Produkt nie jest już dostępny')]",
                        "//*[@class='product-card__message']",
                        "//*[contains(@class,'product-status__item text-alert')]",
                        "//div[contains(@class, 'alert-box')]//div[contains(text(), 'niedostępny')]",
                        "//div[contains(@class, 'product-info__availability')]//span[contains(text(), 'niedostępny')]"
                    ]
                    
                    is_out_of_stock = any(self.element_exists(tree, selector) for selector in out_of_stock_selectors)
                    
                    if is_out_of_stock:
                        stock_status = "Outstock"
                        price = "0.00"
                    else:
                        stock_status = "Instock"
                
                remaining = len(product_rows) - (product_index + 1)
                self.log_info(f"[{product_index+1}/{len(product_rows)}] {url_id}: {remaining} remaining")
                self.log_info(f"Price: {price} PLN , Stock: {stock_status}, url: {url}")
                # Create the product item
                item = ProductItem()
                item["price_link"] = url
                item["xpath_result"] = price
                item["out_of_stock"] = stock_status
                item["market_player"] = self.market_player
                if "BNCode" in row:
                    item["bn_code"] = row["BNCode"]
                
                # Add to our processed products
                self.processed_products.append(item)
                
            except Exception as e:
                self.log_error(f"⚠️ Error processing {url_id}: {str(e)}")
                # Create an empty item for failed products
                item = ProductItem()
                item["price_link"] = url
                item["xpath_result"] = ""
                item["out_of_stock"] = ""
                item["market_player"] = self.market_player
                if "BNCode" in row:
                    item["bn_code"] = row["BNCode"]
                
                self.processed_products.append(item)
        else:
            # All retries failed, create an empty item
            self.log_error(f"⚠️ All retries failed for {url_id}")
            item = ProductItem()
            item["price_link"] = url
            item["xpath_result"] = ""
            item["out_of_stock"] = ""
            item["market_player"] = self.market_player
            if "BNCode" in row:
                item["bn_code"] = row["BNCode"]
            
            self.processed_products.append(item)
        
        # Move to the next product
        next_index = product_index + 1
        next_meta = {
            "product_rows": product_rows,
            "product_index": next_index
        }
        
        # Continue with the next product
        yield scrapy.Request(
            url="https://example.com/dummy",
            callback=self.process_product,
            meta=next_meta,
            dont_filter=True
        )
        
    def parse(self, response):
        """
        Placeholder parse method - not used directly.
        All processing happens in process_product.
        """
        pass 