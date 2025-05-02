from src.spiders.base_spider import BaseSpider
from src.items import ProductItem
from src.utils.proxy_manager import ProxyManagerFreeProxyListDotNet
from src.utils.price_formatter import format_pl_price
import scrapy
import logging
import json
import random
import time

class CustomAmazonMiddleware:
    """
    Custom middleware to help bypass Amazon's anti-bot protection.
    This middleware adds random delays, rotates user agents, and
    manages cookies and headers to make requests look more like
    a real browser.
    """
    
    def __init__(self):
        self.cookies = {}
    
    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls()
        return middleware
        
    def process_request(self, request, spider):
        # Only apply to Amazon requests
        if spider.name != 'amazon':
            return None
            
        # Make the request look like coming from Google search
        if not request.headers.get('Referer'):
            url_id = request.meta.get('url_id', 'product')
            request.headers['Referer'] = f'https://www.google.com/search?q={url_id}+amazon+pl'.encode()
        
        # Add random accept header variations
        accept_variations = [
            'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        ]
        request.headers['Accept'] = random.choice(accept_variations).encode()
        
        # Add random accept-language variations
        lang_variations = [
            'en-US,en;q=0.9,pl;q=0.8',
            'pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7',
            'en-GB,en;q=0.9,pl;q=0.8'
        ]
        request.headers['Accept-Language'] = random.choice(lang_variations).encode()
        
        # Add random viewport sizes to make each request unique
        width = random.randint(1024, 1920)
        request.headers['Viewport-Width'] = str(width).encode()
        request.headers['Sec-Ch-Viewport-Width'] = str(width).encode()
        
        # Add random connection parameters
        request.headers['Downlink'] = str(random.randint(5, 15)).encode()
        request.headers['Rtt'] = str(random.randint(50, 150)).encode()
        
        # Add browser-like headers
        request.headers['Sec-Fetch-Mode'] = b'navigate'
        request.headers['Sec-Fetch-Site'] = b'none'
        request.headers['Sec-Fetch-User'] = b'?1'
        request.headers['Sec-Fetch-Dest'] = b'document'
        request.headers['Priority'] = b'u=0, i'
        
        return None

    def process_response(self, request, response, spider):
        # Only handle Amazon responses
        if spider.name != 'amazon':
            return response
            
        # If we get a 202, retry the request with different parameters
        if response.status == 202 and not getattr(request, 'dont_retry', False):
            # Check if we've already retried too many times
            retry_count = request.meta.get('middleware_retry_count', 0)
            if retry_count >= 3:
                spider.logger.warning(f"Middleware retry limit reached for {request.url}")
                return response
                
            # Create a new request with modifications to bypass protection
            new_request = request.copy()
            new_request.meta['middleware_retry_count'] = retry_count + 1
            new_request.dont_filter = True
            
            # Add a cache-busting parameter
            url = request.url
            if '?' not in url:
                new_request = new_request.replace(url=f"{url}?_={int(time.time())}")
            else:
                new_request = new_request.replace(url=f"{url}&_={int(time.time())}")
                
            # Use a different user agent
            if hasattr(spider, 'get_headers_with_random_ua'):
                new_headers = spider.get_headers_with_random_ua()
                for key, value in new_headers.items():
                    if key.lower() != 'content-length':
                        new_request.headers[key] = value
            
            spider.logger.info(f"Middleware retrying 202 response (attempt {retry_count+1}/3)")
            return new_request
            
        return response

class Amazon(BaseSpider):
    name = "amazon"
    market_player = "Amazon"
    use_proxy_manager = True  # Enable proxy manager
    use_random_ua = True
    use_human_like_delay = True

    # Custom sleep time range for human-like delays
    min_sleep_time = 20  # Increase minimum sleep time in seconds
    max_sleep_time = 25  # Increase maximum sleep time in seconds
    between_products_delay = 1  # Increase delay between products
    between_retries_delay = 1  # Increase delay before retrying with a new proxy

    custom_settings = {
        **BaseSpider.custom_settings,
        'DOWNLOAD_DELAY': 3,  # Increase delay
        'DOWNLOAD_TIMEOUT': 30,  # Increase timeout
        'HTTPERROR_ALLOW_ALL': False,
        'HTTPERROR_ALLOWED_CODES': [202, 404, 410, 429, 503],
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_ENABLED': True,
        'RETRY_TIMES': 5,  # Increase retry times
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429, 202],
        'ROBOTSTXT_OBEY': False,  # Don't obey robots.txt
        # Add our custom middleware to the beginning of the downloader middleware chain
        'DOWNLOADER_MIDDLEWARES': {
            __name__ + '.CustomAmazonMiddleware': 543,
        },
    }

    # Amazon-specific headers with detailed browser fingerprinting
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,pl;q=0.8',
        'cache-control': 'max-age=0',
        'device-memory': '8',
        'downlink': '10',
        'dpr': '1.25',
        'ect': '4g',
        'rtt': '50',
        'sec-ch-device-memory': '8',
        'sec-ch-dpr': '1.25',
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"19.0.0"',
        'sec-ch-viewport-width': '1041',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
        'viewport-width': '1041',
        'cookie': 'csm-sid=187-3064766-6848691; x-amz-captcha-1=1745062953022502; x-amz-captcha-2=GtcDmFY20U5CQMx2iGVprg==; session-id=259-3804135-9831462; i18n-prefs=PLN; ubid-acbpl=259-0848705-1476648; session-token=yymjfl2uuAQtBQauO0CM/sk+oMrmcPeu7EMmswkprkppok7+5PmK1fQhWMr0dVJwX6844ej+ObHCB/F2WhnQlYTvuEXdLnMGo2o6j+FoAiL7a+lCRxpKiQmthCBzEKXJAsVb+9yHNGi2k+8ovgVp0/4Uygedr5vTHtHfltSQN1/6dm/2qIwgsSrPXRvQuXoitSjA4BeeFHRrMzdZN74WA3rrtrd5crievxYMiEKAed80iPaCfacNCPDJF0kSbfjfPoh+xw42DBWK+wtyrl0exDnIXEJhPgjyPJDOKwcAM/QOsuPQcPAFXtSEYj3DOt105orp+PWkKqZ+ZYGxz51ZZrhgKehf8YSZ; JSESSIONID=C73D948A4BE0DD34CD4583B282F2E134; session-id-time=2082787201l; csm-hit=tb:GTYSNXR5T44Z0PZ83D1Y+s-4DTA6PS22BHVBD3RVD8A|1745069510687&t:1745069510687&adb:adblk_no; aws-waf-token=c0e7f683-71f6-4d3a-bb00-5ff59ff330ff:BgoAj3ReTpkdAAAA:40pbEyjWzscueHpjBkU3ntHTOBqpIT+fmBfgyc7u9QO0JDBN/FxiHUKe6b+14jMZj1XFbL78QdTraUDsELlTXFKTjN0nGuqaYOcMorgEYrd0lGaSyAcmOCD2aCAXYEtmnAC5Rw5c2blaHFh+usrYzlH45qJLENFA46TjAyY9WB9aGej1RumphRBqujLFHbZE',
    }
        
    def __init__(self, input_data=None, *args, **kwargs):
        super().__init__(input_data, *args, **kwargs)
    
    def element_exists(self, response, xpath):
        return len(response.xpath(xpath)) > 0
        
    def format_price(self, price_text):
        if price_text:
            return format_pl_price(price_text)
        return ""
        
    def is_validation_page(self, response):
        """Amazon specific validation page detection"""
        # Use the base implementation first
        if super().is_validation_page(response):
            return True
            
        return False

    def parse(self, response):
        row = response.meta["row"]
        product_rows = response.meta.get("product_rows", [])
        product_index = response.meta.get("product_index", 0)
        price_link = row["PriceLink"]
        url_id = row.get("BNCode", "unknown")
        retry_count = response.meta.get("retry_count", 0)
        
        # Handle 202 Accepted status (Amazon anti-bot response)
        if response.status == 202:
            # Max retries to prevent infinite loops
            if retry_count >= 5:
                self.log_error(f"⚠️ Max retries exceeded for {url_id} after 202 status code")
                
                # Try with proxy if not already using proxy manager
                if not self.use_proxy_manager and self.proxy_manager:
                    self.log_info(f"⚠️ Switching to proxy approach for {url_id} after exhausting direct retries")
                    # Create a temporary proxy manager just for this request
                    temp_proxies_list = [{"proxy": self.proxy_manager.own_proxy, "is_https": False}]
                    
                    proxy = self.proxy_manager.own_proxy
                    proxy_url = f"http://{proxy}"
                    
                    # Get fresh headers with a new random user agent
                    headers = self.get_headers_with_random_ua()
                    
                    self.log_info(f"🔄 Trying with own HTTP proxy: {proxy}")
                    
                    yield scrapy.Request(
                        url=response.url,
                        callback=self.parse,
                        headers=headers,
                        meta={
                            "row": row,
                            "product_rows": product_rows,
                            "product_index": product_index,
                            "proxy": proxy_url,
                            "retry_count": 0,  # Reset retry count for proxy approach
                            "using_emergency_proxy": True,
                            "url_id": url_id
                        },
                        dont_filter=True
                    )
                    return
                
                # If we're already using proxies or tried emergency proxy, mark as error
                if response.meta.get("using_emergency_proxy"):
                    self.log_error(f"⚠️ Even proxy approach failed for {url_id}")
                
                # Fallback: return as unavailable
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
            backoff_time = 30 * (2 ** retry_count)  # 30s, 60s, 120s, 240s, 480s
            self.log_warning(f"⚠️ Amazon anti-bot response (202) for {url_id}. Retry {retry_count+1}/5 after {backoff_time} seconds")
            
            # Schedule a retry with increased delay
            meta = response.meta.copy()
            meta["retry_count"] = retry_count + 1
            
            # Get fresh headers with a new random user agent
            headers = self.get_headers_with_random_ua()
            
            # Add a referrer that looks like we came from Google
            headers['referer'] = f"https://www.google.com/search?q={url_id}+amazon+pl"
            
            # Add randomized query parameter to avoid cache
            retry_url = response.url
            if "?" not in retry_url:
                retry_url += f"?_={int(time.time())}"
            else:
                retry_url += f"&_={int(time.time())}"
            
            # Use time.sleep for the backoff
            time.sleep(backoff_time)
            
            # Additional randomized wait to make it look more human
            extra_wait = random.uniform(1, 5)
            time.sleep(extra_wait)
            
            yield scrapy.Request(
                url=retry_url,
                callback=self.parse,
                headers=headers,
                meta=meta,
                cookies={},  # Reset cookies
                dont_filter=True
            )
            return
        
        # Handle other error responses or validation pages
        if response.status != 200 or self.is_validation_page(response):
            proxy_manager = response.meta.get("proxy_manager", self.proxy_manager)
            if self.use_proxy_manager and proxy_manager:
                # Use yield from to properly handle the generator from try_next_proxy
                next_request = proxy_manager.try_next_proxy(response)
                if next_request:
                    yield next_request
                    return  # Return without value after yielding
                return  # Important: return after yielding from generator
            else:
                self.log_error(f"⚠️ Error or validation page detected (status: {response.status}) for {url_id}")
                
                # Try switching to proxy approach
                if not response.meta.get("using_emergency_proxy") and self.proxy_manager:
                    self.log_info(f"⚠️ Switching to emergency proxy for {url_id}")
                    
                    proxy = self.proxy_manager.own_proxy
                    proxy_url = f"http://{proxy}"
                    
                    # Get fresh headers with a new random user agent
                    headers = self.get_headers_with_random_ua()
                    
                    yield scrapy.Request(
                        url=response.url,
                        callback=self.parse,
                        headers=headers,
                        meta={
                            "row": row,
                            "product_rows": product_rows,
                            "product_index": product_index,
                            "proxy": proxy_url,
                            "retry_count": 0,
                            "using_emergency_proxy": True,
                            "url_id": url_id
                        },
                        dont_filter=True
                    )
                    return
                
                # Fallback: return as unavailable
                item = ProductItem()
                item["price_link"] = price_link
                item["xpath_result"] = "0.00"
                item["out_of_stock"] = "Outstock"
                item["market_player"] = self.market_player
                if "BNCode" in row:
                    item["bn_code"] = row["BNCode"]
                yield item
                return

        try:
            # Log successful response
            self.log_info(f"✅ Successfully got 200 response for {url_id} (Size: {len(response.body)} bytes)")
            
            price = ""
            stock_status = ""

            # First, check if the product has any offer
            has_any_offer = self.element_exists(response, "//div[@id='aod-offer']")
            
            # Check if the product is sold and shipped by Amazon
            sold_and_shipped_by_amazon_aod_offer = self.element_exists(response, "//div[@id='aod-offer'][.//div[@id='aod-offer-shipsFrom'][.//span[contains(text(),'Amazon')]]/following-sibling::div[@id='aod-offer-soldBy'][.//span[contains(text(),'Amazon')]]]")
            
            sold_and_shipped_by_amazon_buybox = self.element_exists(response, "//div[@id='fulfillerInfoFeature_feature_div'][.//span[contains(text(),'Amazon')]]/following-sibling::div[@id='merchantInfoFeature_feature_div'][.//span[contains(text(),'Amazon')]]")
            
            self.log_info(f"Searching for sold and shipped by Amazon (AOD): {sold_and_shipped_by_amazon_aod_offer}")
            self.log_info(f"Searching for sold and shipped by Amazon (Buybox): {sold_and_shipped_by_amazon_buybox}")
            # For this implementation, we'll consider a product as "InStock" ONLY if it's sold AND shipped by Amazon
            if sold_and_shipped_by_amazon_buybox:
                stock_status = "InStock"
                
                # Extract price for Amazon-sold products
                price_xpath = "//div[@id='buyBoxAccordion']/div[re:test(@id, 'newAccordionRow')][.//div[@id='fulfillerInfoFeature_feature_div'][.//span[text()='Amazon']]/following-sibling::div[@id='merchantInfoFeature_feature_div'][.//span[text()='Amazon']]]//span[@class='a-offscreen']/text()"
                raw_price = self.extract_data(response, price_xpath)
                if not raw_price:
                    price_xpath = "//div[@id='qualifiedBuybox'][.//div[@id='fulfillerInfoFeature_feature_div'][.//span[text()='Amazon']]/following-sibling::div[@id='merchantInfoFeature_feature_div'][.//span[text()='Amazon']]]//span[@class='a-offscreen']/text()"
                    raw_price = self.extract_data(response, price_xpath)
                price = format_pl_price(raw_price)

            elif sold_and_shipped_by_amazon_aod_offer:
                stock_status = "InStock"
                
                # Extract price for Amazon-sold products
                price_xpath = "//div[@id='aod-offer'][./div[@id='aod-offer-shipsFrom'][.//span[contains(text(),'Amazon')]]/following-sibling::div[@id='aod-offer-soldBy'][.//span[contains(text(),'Amazon')]]]/div[@id='aod-offer-price']//span[@class='aok-offscreen']/text()"
                raw_price = self.extract_data(response, price_xpath)
                price = format_pl_price(raw_price)
                
                # If that fails, try alternate price xpath
                if not price:
                    price_xpath = "//span[@id='aod-price-0']/div/span/span[1]/text() | //div[@id='aod-price-0']/span/span/text()"
                    raw_price = self.extract_data(response, price_xpath)
                    price = format_pl_price(raw_price)
            else:
                # Get current headers from response.meta or use current Spider's default headers
                current_headers = response.meta.get("headers", self.headers)
                
                # If not found in the main buybox or AOD section on the page, check via the AOD API
                sold_by_amazon, aod_price, error_msg = self.check_aod_sold_by_amazon(
                    response.url,
                    current_headers
                )
                if sold_by_amazon:
                    price = aod_price
                    stock_status = "InStock"
                else:
                    # Product is not sold AND shipped by Amazon, so mark as OutStock
                    stock_status = "OutStock"
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
        
        # Process next product if proxy manager is enabled
        if self.use_proxy_manager:
            proxy_manager = response.meta.get("proxy_manager", self.proxy_manager)
            next_product_index = product_index + 1
            next_request = proxy_manager.process_next_product(response, next_product_index)
            if next_request:
                yield next_request

    def check_aod_sold_by_amazon(self, url, headers):
        """
        Check if the product is sold and shipped by Amazon using the AOD API
        Uses the proxy manager if enabled
        
        Args:
            url (str): Product URL
            headers (dict): Headers from the original request
            
        Returns:
            tuple: (is_sold_by_amazon, price, error_message)
        """
        # If proxy manager is enabled, use it to make this request
        if self.use_proxy_manager and hasattr(self, 'proxy_manager') and self.proxy_manager:
            # Create a new Scrapy request for the AOD API via the proxy manager
            import re
            
            # Extract ASIN from the URL
            asin_match = re.search(r'/dp/([A-Z0-9]{10})', url)
            if asin_match:
                asin = asin_match.group(1)
            else:
                parts = url.split('/dp/')
                if len(parts) > 1:
                    asin = parts[1].split('/')[0]
                    asin = asin.split('?')[0]  # Remove query parameters
                else:
                    self.log_error(f"Could not extract ASIN from URL: {url}")
                    return False, "", "Could not extract ASIN"
            
            self.log_info(f"Using proxy manager to check AOD for ASIN: {asin}")
            
            # Create AOD API URL with parameters
            from urllib.parse import urlencode
            params = {
                'asin': asin,
                'pc': 'dp',
                'experienceId': 'aodAjaxMain',
            }
            aod_url = f'https://www.amazon.pl/gp/product/ajax/ref=auto_load_aod?{urlencode(params)}'
            
            # Prepare headers for the AOD request
            aod_headers = headers.copy() if isinstance(headers, dict) else {}
            aod_headers['accept'] = 'text/html,*/*'
            aod_headers['x-requested-with'] = 'XMLHttpRequest'
            aod_headers['referer'] = url
            
            # Try to get the current proxy
            current_proxy = None
            if hasattr(self.proxy_manager, 'current_proxy') and self.proxy_manager.current_proxy:
                current_proxy = self.proxy_manager.current_proxy
                self.log_info(f"Got current proxy from manager: {current_proxy['proxy']}")
            else:
                # Try to get the proxy directly from the proxy manager's proxies list
                if hasattr(self.proxy_manager, 'proxies') and self.proxy_manager.proxies:
                    if len(self.proxy_manager.proxies) > 0:
                        current_proxy = self.proxy_manager.proxies[0]
                        self.log_info(f"Using first proxy from list: {current_proxy['proxy']}")
                    else:
                        self.log_error("Proxy manager has no proxies in list!")
                # If we still have no proxy, use the own_proxy directly
                if not current_proxy and hasattr(self.proxy_manager, 'own_proxy') and self.proxy_manager.own_proxy:
                    current_proxy = {'proxy': self.proxy_manager.own_proxy, 'is_https': False}
                    self.log_info(f"Using own proxy directly: {current_proxy['proxy']}")
            
            if not current_proxy:
                self.log_error("Failed to get any proxy from proxy manager!")
                return False, "", "No proxy available from proxy manager"
            
            # Use synchronous proxy manager approach
            try:
                import requests
                from scrapy.selector import Selector
                time.sleep(random.uniform(1, 5))  # Add delay to avoid rate limiting
                
                # Use the proxy directly
                proxy_url = f"http://{current_proxy['proxy']}"
                proxies = {'http': proxy_url, 'https': proxy_url}
                
                self.log_info(f"Making AOD API request to {aod_url} with proxy: {current_proxy['proxy']}")
                
                try:
                    r = requests.get(
                        aod_url,
                        headers=aod_headers,
                        proxies=proxies,
                        timeout=30
                    )
                    
                    self.log_info(f"AOD API proxy request status: {r.status_code}")
                    
                    if r.status_code == 200:
                        self.log_info(f"AOD API proxy request successful. Response length: {len(r.text)} bytes")
                        response = Selector(text=r.text)
                        sold_and_shipped_by_amazon_aod_offer = self.element_exists(response, "//div[@id='aod-offer'][.//div[@id='aod-offer-shipsFrom'][.//span[contains(text(),'Amazon')]]/following-sibling::div[@id='aod-offer-soldBy'][.//span[contains(text(),'Amazon')]]]")
                        
                        self.log_info(f"Searching for sold and shipped by Amazon (AOD): {sold_and_shipped_by_amazon_aod_offer}")
                        
                        if sold_and_shipped_by_amazon_aod_offer:
                            price_xpath = "//div[@id='aod-offer'][./div[@id='aod-offer-shipsFrom'][.//span[contains(text(),'Amazon')]]/following-sibling::div[@id='aod-offer-soldBy'][.//span[contains(text(),'Amazon')]]]//div[@id='aod-offer-price']//span[@class='aok-offscreen']/text()"
                            raw_price = self.extract_data(response, price_xpath)
                            price = format_pl_price(raw_price)
                            
                            self.log_info(f"Extracted price: {price} from raw: {raw_price}")
                            
                            if not price:
                                # Try alternate price xpath
                                price_xpath = "//div[@id='aod-offer'][./div[@id='aod-offer-shipsFrom'][.//span[contains(text(),'Amazon')]]/following-sibling::div[@id='aod-offer-soldBy'][.//span[contains(text(),'Amazon')]]]//span[contains(@class,'a-offscreen')]/text()"
                                raw_price = self.extract_data(response, price_xpath)
                                price = format_pl_price(raw_price)
                                self.log_info(f"Alternate price extraction: {price} from raw: {raw_price}")
                            
                            if price:
                                self.log_info(f"Found Amazon as seller & shipper via proxy with price: {price}")
                                return True, price, ""
                        else:
                            self.log_info("Amazon not found as seller and shipper in AOD response")
                    
                    # Handle error or not found scenario
                    if r.status_code != 200:
                        self.log_info(f"AOD API proxy request failed with status {r.status_code}")
                
                except requests.exceptions.RequestException as req_error:
                    self.log_error(f"Request error with proxy {current_proxy['proxy']}: {str(req_error)}")
                
                # Try with a different proxy if available
                self.log_info("Trying with a different proxy...")
                
                try:
                    # Try to get next proxy
                    new_proxy = None
                    
                    # Check if we have a direct method to get the next proxy
                    if hasattr(self.proxy_manager, 'try_get_next_proxy'):
                        self.log_info("Using try_get_next_proxy method")
                        new_proxy = self.proxy_manager.try_get_next_proxy()
                    # If not, try to get a proxy from the list
                    elif hasattr(self.proxy_manager, 'proxies') and len(self.proxy_manager.proxies) > 1:
                        # Get a different proxy than the current one
                        for proxy in self.proxy_manager.proxies:
                            if proxy['proxy'] != current_proxy['proxy']:
                                new_proxy = proxy
                                self.log_info(f"Selected different proxy from list: {new_proxy['proxy']}")
                                break
                    
                    if new_proxy:
                        proxy_url = f"http://{new_proxy['proxy']}"
                        proxies = {'http': proxy_url, 'https': proxy_url}
                        
                        self.log_info(f"Retrying AOD API with next proxy: {new_proxy['proxy']}")
                        
                        # Add more delay before retry
                        time.sleep(random.uniform(3, 6))
                        
                        r = requests.get(
                            aod_url,
                            headers=aod_headers,
                            proxies=proxies,
                            timeout=30
                        )
                        
                        self.log_info(f"Second proxy attempt status: {r.status_code}")
                        
                        if r.status_code == 200:
                            self.log_info(f"Second proxy response length: {len(r.text)} bytes")
                            response = Selector(text=r.text)
                            sold_and_shipped_by_amazon_aod_offer = self.element_exists(response, "//div[@id='aod-offer'][.//div[@id='aod-offer-shipsFrom'][.//span[contains(text(),'Amazon')]]/following-sibling::div[@id='aod-offer-soldBy'][.//span[contains(text(),'Amazon')]]]")
                            
                            self.log_info(f"Second attempt - sold by Amazon: {sold_and_shipped_by_amazon_aod_offer}")
                            
                            if sold_and_shipped_by_amazon_aod_offer:
                                price_xpath = "//div[@id='aod-offer'][./div[@id='aod-offer-shipsFrom'][.//span[contains(text(),'Amazon')]]/following-sibling::div[@id='aod-offer-soldBy'][.//span[contains(text(),'Amazon')]]]//div[@id='aod-offer-price']//span[@class='aok-offscreen']/text()"
                                raw_price = self.extract_data(response, price_xpath)
                                price = format_pl_price(raw_price)
                                
                                self.log_info(f"Second attempt price: {price}")
                                
                                if price:
                                    self.log_info(f"Found Amazon as seller & shipper with next proxy, price: {price}")
                                    return True, price, ""
                    else:
                        self.log_info("No alternative proxy available")
                
                except Exception as proxy_error:
                    self.log_error(f"Error with alternative proxy: {str(proxy_error)}")
                    
                # If direct proxy approach failed or we don't have a current proxy
                self.log_info("All proxy attempts failed for AOD check")
                return False, "", "AOD check via proxy failed or Amazon not seller"
                
            except Exception as e:
                self.log_error(f"Error in AOD proxy check: {str(e)}")
                return False, "", f"Proxy error: {str(e)}"
        
        # If proxy manager not enabled or not available, use direct request
        import requests
        import re
        time.sleep(random.uniform(1, 5))
        try:
            # Extract ASIN from the URL
            asin_match = re.search(r'/dp/([A-Z0-9]{10})', url)
            if asin_match:
                asin = asin_match.group(1)
            else:
                # Fallback method if regex fails
                parts = url.split('/dp/')
                if len(parts) > 1:
                    asin = parts[1].split('/')[0]
                    asin = asin.split('?')[0]  # Remove query parameters
                else:
                    self.log_error(f"Could not extract ASIN from URL: {url}")
                    return False, "", "Could not extract ASIN"
           
            self.log_info(f"Checking AOD for ASIN: {asin}")
            
            params = {
                'asin': asin,
                'pc': 'dp',
                'experienceId': 'aodAjaxMain',
            }

            self.log_info(f"Making AOD API request for ASIN: {asin}")
            headers['referer'] = url
            r = requests.get(
                'https://www.amazon.pl/gp/product/ajax/ref=auto_load_aod',
                params=params,
                headers=headers,
                timeout=30
            )
            
            self.log_info(f"AOD API request status: {r.status_code}")
            
            if r.status_code == 200:
                response = scrapy.Selector(text=r.text)

                sold_and_shipped_by_amazon_aod_offer = self.element_exists(response, "//div[@id='aod-offer'][.//div[@id='aod-offer-shipsFrom'][.//span[contains(text(),'Amazon')]]/following-sibling::div[@id='aod-offer-soldBy'][.//span[contains(text(),'Amazon')]]]")
                print('sold_and_shipped_by_amazon_aod_offer',sold_and_shipped_by_amazon_aod_offer)
                # Extract price for Amazon-sold products
                price_xpath = "//div[@id='aod-offer'][./div[@id='aod-offer-shipsFrom'][.//span[contains(text(),'Amazon')]]/following-sibling::div[@id='aod-offer-soldBy'][.//span[contains(text(),'Amazon')]]]//div[@id='aod-offer-price']//span[@class='aok-offscreen']/text()"
                raw_price = self.extract_data(response, price_xpath)
                price = format_pl_price(raw_price)
                if price:
                    return True, price, ""
            elif r.status_code == 202:
                self.log_info(f"AOD API returned 202 status, likely anti-bot measure")
                    
            return False, "", f"AOD check failed or not sold by Amazon (status: {r.status_code})"
            
        except Exception as e:
            self.log_error(f"Error in AOD check: {str(e)}")
            return False, "", f"Error: {str(e)}"

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
                
                # Add a random sleep time if human-like delay is enabled
                if self.use_human_like_delay:
                    sleep_time = random.uniform(self.min_sleep_time, self.max_sleep_time)
                    self.log_info(f"😴 Sleeping for {sleep_time:.2f} seconds before request {i+1}/{len(product_rows)}")
                    time.sleep(sleep_time)
                
                # Get headers with a random user agent
                headers = self.get_headers_with_random_ua()
                
                # Meta with Amazon-specific settings
                meta = {
                    "row": row,
                    "product_rows": product_rows,
                    "product_index": i,
                    "handle_httpstatus_list": [202, 404, 410, 429, 503],  # Add 202 to handled status codes
                    "retry_count": 0,  # Track retries
                    "url_id": url_id,
                    "headers": headers,
                }
                
                self.log_info(f"🚀 Sending request {i+1}/{len(product_rows)} to {url} for {url_id}")
                
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    headers=headers,
                    meta=meta,
                    dont_filter=True
                ) 