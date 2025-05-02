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

class OleoleBackup(BaseSpider):
    name = "oleole_backup"
    market_player = "Oleole"
    use_proxy_manager = True  # Enable using the proxy manager
    use_human_like_delay = True

    # Custom sleep time range for human-like delays
    min_sleep_time = 40  # Minimum sleep time in seconds
    max_sleep_time = 50  # Maximum sleep time in seconds
    between_products_delay = 3  # Additional delay between products when using proxy manager
    between_retries_delay = 1  # Delay before retrying with a new proxy after failure
    if use_proxy_manager:
        min_sleep_time = 1
        max_sleep_time = 2
        between_products_delay = 0
        between_retries_delay = 0
    # Retry settings (for non-proxy manager mode)
    max_retries = 3  # Maximum retries per individual URL
    retry_delay_base = 30  # Base delay in seconds (will be used for exponential backoff)
        
    custom_settings = {
        **BaseSpider.custom_settings,
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 1,
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
        # 'cookie': '__uzma=08736f66-50ed-4a79-ae1c-57dbe0b3ddfe; __uzmb=1744187813; __uzme=5678; navTestAB=B; searchTestAB=V; __ssds=2; __ssuzjsr2=a9be0cd8e; __uzmbj2=1744187924; __uzmlj2=U6qY5Qh3CVizGzUuz3pUleWAIrdhuy7bL0Y4k04KFbk=; _pfbuid=472e57ea-45ce-2130-6799-53a8291f4961; __uzmaj2=08736f66-50ed-4a79-ae1c-57dbe0b3ddfe; userId=0; OptanonAlertBoxClosed=2025-04-10T07:04:15.162Z; _gcl_au=1.1.1187886174.1744268655; _ga=GA1.1.1969487444.1744268656; _fbp=fb.1.1744268656884.486830022752307560; _tt_enable_cookie=1; _ttp=01JRF870C462XP4N5ZD0W5Q3X9_.tt.1; _uetvid=7ff8f6401a7611f0b98faf0bc12cd304; _prefixboxIntegrationVariant=; _pfbx_application_information=ab145fb3-6f86-4498-bf5d-b18b367943a1; __Host-JSESSIONID=84D4F64866C56BE7FF01404411FFC5D3.inst1ap82; oleLogged=false; oleCustomer=1570104530::20; oleVisit=1570139402::::false::::0::0::1570104530::0::::; _pfbsesid=896ab9e8-be9c-0725-ed08-a174b59cf0d4; __uzmcj2=1451017275389; __uzmdj2=1745302845; __uzmfj2=7f600065c52110-5652-4c70-bba8-5eaa52a88f4f17441879242681114921338-826662bb5e5a5487172; uzmxj=7f900084629b39-85c3-4dc1-828f-393b4820966411-17441879242681114921338-4ecbc14bd3bbad12172; _ga_9EEHE5KH14=GS1.1.1745302848.26.1.1745302848.60.0.0; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22QBFmtrJAuAIKEn5ZP3qi%22%2C%22expiryDate%22%3A%222026-04-22T06%3A20%3A48.657Z%22%7D; __uzmc=6845755023398; __uzmd=1745302850; __uzmf=7f600065c52110-5652-4c70-bba8-5eaa52a88f4f17441878132771115036892-2fa73644f6a68649550; uzmx=7f900084629b39-85c3-4dc1-828f-393b4820966411-17441878132771115036892-06562d8fd8c9bb31550; ttcsid_CL0FLU3C77UDR4OH86N0=1745302850528.23.1745302850528; ttcsid=1745302850528.21.1745302850528; _clck=s414zt%7C2%7Cfva%7C0%7C1926; cto_bundle=j-9IJF9lSzExNWVGMWdXbDFucTlmUVJJUjEwc2poeWtqWnp5UWVxdjY4SGlOVFFNWiUyQm1oZEF0RlFqVzcxR1NqU3YwalpNejgxTUFjd2NUZDNpbnptQlVOTDBJTkVqZVJBTm03Rng1eFVvMHNGWkFRSmphd3R5b2ZLZiUyQnIzY1Y5cHYyayUyQiUyQkRXcVZmJTJGYmlxOGplVjZ1ZSUyRjg0elhzbU4wOFFTTXZCU0h2M1hmV2RNcDNjVHdkWDI1MDNaJTJCMGVCWElzWkMlMkJOWlAlMkY5blUyczglMkJqWEtDSDZ5VE92WGclM0QlM0Q; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Apr+22+2025+13%3A20%3A51+GMT%2B0700+(Indochina+Time)&version=202502.1.0&browserGpcFlag=0&isIABGlobal=false&consentId=ea145b1e-fcd9-461e-b0c9-e884876bd74f&interactionCount=2&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1&hosts=H9%3A1%2CH124%3A1%2CH41%3A1%2CH169%3A1%2CH103%3A1%2CH150%3A1%2CH36%3A1%2CH179%3A1%2CH204%3A1%2CH206%3A1%2CH175%3A1%2CH178%3A1%2CH187%3A1%2CH158%3A1%2CH6%3A1%2CH188%3A1%2CH159%3A1%2CH161%3A1%2CH181%3A1%2CH163%3A1%2CH192%3A1%2CH193%3A1%2CH21%3A1%2CH59%3A1%2CH23%3A1%2CH167%3A1%2CH122%3A1%2CH170%3A1%2CH198%3A1%2CH152%3A1%2CH32%3A1%2CH35%3A1&genVendors=&AwaitingReconsent=false&intType=1&geolocation=SE%3BAB; _clsk=8azw5f%7C1745302852279%7C1%7C1%7Cu.clarity.ms%2Fcollect',
    }

    def __init__(self, input_data=None, *args, **kwargs):
        super().__init__(input_data, *args, **kwargs)
        # Use the Poland-specific proxy manager instead of the default one
        if self.use_proxy_manager:
            self.proxy_manager = ProxyManagerFreeProxyListDotNet()
            self.proxy_manager.logger = self.custom_logger
            # Set minimum proxy count for testing
            self.proxy_manager.min_proxy_count = 2 
            
            # Store cookies per domain and proxy
            self.cookies_jar = {}
        
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
                    sleep_time = random.uniform(1, 3)
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
                price = data['offers']['price']
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
                
            # Concise logging of just the essential information
            remaining = len(product_rows) - (product_index + 1)
            self.log_info(f"[{product_index+1}/{len(product_rows)}] {url_id}: {price} PLN | {stock_status} | {remaining} remaining")
                
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
        yield item
        
        if self.use_proxy_manager:
            # Handle next product with proxy manager
            next_product_index = product_index + 1
            proxy_manager = response.meta.get("proxy_manager", self.proxy_manager)
            next_request = proxy_manager.process_next_product(response, next_product_index)
            if next_request:
                yield next_request

    