# src/utils/proxy_manager.py
import logging
import random
import scrapy
import time
import requests
import json
import re

# ProxyManagerFreeProxyListDotNet

class ProxyManagerFreeProxyListDotNet:
    def __init__(self):
        self.valid_proxies = []
        self.current_proxy = None
        # Use a shorter logger name to make logs cleaner
        self.logger = logging.getLogger("proxy")
        
        # Add your own proxy to use first
        self.own_proxy = "192.168.101.27:8080"  # Your own proxy that will be tried first
        
        # Define the URL for fetching proxies
        self.proxy_list_url = 'https://free-proxy-list.net/'
        
        # Minimum number of proxies before retrying
        self.min_proxy_count = 5
        
        # Maximum proxy refresh attempts
        self.max_proxy_refresh_attempts = 3 # try 3 times with proxy_list_url if public proxies < 5
        self.current_refresh_attempt = 0
        self.limit_proxy_list = 100
        
        # Global proxy refresh counter - to terminate after too many global refreshes
        self.global_proxy_refresh_count = 0
        self.max_global_proxy_refreshes = 4  # Maximum number of times to try refreshing proxies globally

    def start_proxy_fetch(self, callback, spider_instance, product_rows):
        """
        Start the proxy fetching process and then continue with scraping.
        
        Parameters:
        - callback: The callback function to process the product pages
        - spider_instance: Instance of the spider to use for creating requests
        - product_rows: List of product data to scrape
        """
        # Use the spider's custom_logger if available
        if hasattr(spider_instance, 'custom_logger'):
            self.logger = spider_instance.custom_logger
        
        # Always fetch proxies from free-proxy-list.net
        self.logger.info(f"🔄 Getting proxies from {self.proxy_list_url}")

        # Use the spider's proper headers with random UA
        headers = spider_instance.get_headers_with_random_ua() if hasattr(spider_instance, 'get_headers_with_random_ua') else {}
        
        return scrapy.Request(
            url=self.proxy_list_url,
            callback=self.parse_proxy_list,
            headers=headers,
            meta={
                'product_rows': product_rows,
                'callback': callback,
                'spider_instance': spider_instance
            },
            dont_filter=True,
            errback=self.handle_proxy_fetch_error
        )

    def parse_proxy_list(self, response):
        """Parse proxy list page and start the scraping with the first product"""
        product_rows = response.meta.get('product_rows', [])
        callback = response.meta.get('callback')
        spider_instance = response.meta.get('spider_instance')
        
        if not product_rows:
            self.logger.info("No product rows provided to proxy manager")
            return
            
        # Fetch public proxies
        public_proxies = self.fetch_proxies(response)
        
        # Add your own proxy at the beginning of the list (wrapped in dict for compatibility)
        own_proxy_dict = {"proxy": self.own_proxy, "is_https": False}  # Own proxy as HTTP
        
        # Limit proxies according to the limit_proxy_list setting
        public_proxies = public_proxies[:self.limit_proxy_list - 1]  # -1 for your own proxy
        proxies_list = [own_proxy_dict] + public_proxies
        
        self.logger.info(f"Added your own proxy + {len(public_proxies)} public proxies (limited to {self.limit_proxy_list} total)")
        
        if not proxies_list:
            self.logger.info("No proxies found. Cannot proceed with scraping.")
            return
            
        # Start with first product and first proxy (which is your own proxy)
        proxy_info = proxies_list[0]
        proxy = proxy_info["proxy"]
        first_row = product_rows[0]
        
        self.logger.info(f"🔄 Starting with your own proxy: {proxy} (1/{len(proxies_list)})")
        
        # Get headers from spider instance using get_headers_with_random_ua method
        headers = spider_instance.get_headers_with_random_ua() if hasattr(spider_instance, 'get_headers_with_random_ua') else {}

        # Use HTTP for your own proxy, HTTPS for all public proxies
        proxy_url = f"http://{proxy}" if proxy == self.own_proxy else f"https://{proxy}"

        self.logger.info(f"Using proxy URL: {proxy_url}")
        
        yield scrapy.Request(
            url=first_row["PriceLink"],
            callback=callback,
            headers=headers,
            meta={
                "row": first_row,
                "proxy": proxy_url,
                "proxies_list": proxies_list,
                "proxy_index": 0,
                "product_rows": product_rows,
                "product_index": 0,
                "dont_retry": True,
                "handle_httpstatus_list": [403, 404, 429, 500, 502, 503],
                "proxy_manager": self,
                "spider_instance": spider_instance
            },
            errback=self.handle_error,
            dont_filter=True
        )

    def fetch_proxies(self, response):
        """Extract proxy list from the response - including both HTTP and HTTPS proxies"""
        proxies_list = []
        https_count = 0
        http_count = 0
        total_count = 0
        
        # First extract data from the standard table format
        for row in response.xpath('//table[@class="table table-striped table-bordered"]/tbody/tr'):
            ip = row.xpath('./td[1]/text()').get()
            port = row.xpath('./td[2]/text()').get()
            https = row.xpath('./td[7]/text()').get()
            
            total_count += 1
            # Only include HTTPS proxies for better security
            if ip and port:
                if https == 'yes':
                    https_count += 1
                    proxies_list.append({"proxy": f"{ip}:{port}", "is_https": True})
        
        # If no proxies found in the standard format, try the alternative proxylisttable format
        if not proxies_list:
            for row in response.xpath('//table[@id="proxylisttable"]/tbody/tr'):
                ip = row.xpath('./td[1]/text()').get()
                port = row.xpath('./td[2]/text()').get()
                https = row.xpath('./td[7]/text()').get()
                
                total_count += 1
                # Only include HTTPS proxies
                if ip and port and https == 'yes':
                    https_count += 1
                    proxies_list.append({"proxy": f"{ip}:{port}", "is_https": True})
        
        # Last resort: if still no proxies, try extracting any IP:port pattern from the page
        # But in this case, assume they're HTTP only and don't include them
        if not proxies_list:
            self.logger.warning("No HTTPS proxies found in structured format, will not attempt to extract IP patterns")
        
        self.logger.info(f"🔍 Found {https_count} HTTPS proxies out of {total_count} total proxies")
        
        # Randomize the proxy list to avoid detection patterns
        random.shuffle(proxies_list)
        
        # Log the first few proxies for visibility
        if proxies_list:
            for i, proxy_info in enumerate(proxies_list[:5]):  # Show first 5 proxies
                self.logger.info(f"  Proxy #{i+1}: {proxy_info['proxy']} (HTTPS)")
            if len(proxies_list) > 5:
                self.logger.info(f"  ... and {len(proxies_list) - 5} more")
        
        # Return proxies based on the limit_proxy_list setting
        return proxies_list[:self.limit_proxy_list]

    def handle_error(self, failure):
        """Handle errors during the request and try with next proxy"""
        # Extract relevant information from failure meta
        request = failure.request
        proxies_list = request.meta.get('proxies_list', [])
        proxy_index = request.meta.get('proxy_index', 0)
        product_rows = request.meta.get('product_rows', [])
        product_index = request.meta.get('product_index', 0)
        spider_instance = request.meta.get('spider_instance')
        url_id = request.meta.get('url_id', 'unknown')
        
        # Log the error
        error_type = type(failure.value).__name__
        error_msg = str(failure.value)
        proxy = request.meta.get('proxy', 'no proxy')
        orig_url = request.url
        current_proxy_number = proxy_index + 1
        total_proxies = len(proxies_list)
        
        self.logger.error(f"❌ Error [{url_id}]: {error_type} occurred with {proxy}: {error_msg}")
        self.logger.info(f"🔄 Moving to next proxy ({current_proxy_number}/{total_proxies})")
        
        # Try next proxy
        proxy_index += 1
        
        # Get between_retries_delay from spider if available
        between_retries_delay = getattr(spider_instance, 'between_retries_delay', 5)
        
        # Add delay before trying next proxy
        if between_retries_delay > 0:
            self.logger.info(f"⏱️ Waiting {between_retries_delay} seconds before trying next proxy (from {spider_instance.name}'s settings)")
            time.sleep(between_retries_delay)
        
        if proxy_index < len(proxies_list):
            # Get next proxy
            proxy_info = proxies_list[proxy_index]
            proxy = proxy_info["proxy"]
            
            # Use HTTP for your own proxy, HTTPS for all public proxies
            proxy_url = f"http://{proxy}" if proxy == self.own_proxy else f"https://{proxy}"
            
            proxy_type = "own" if proxy == self.own_proxy else "public"
            protocol = "HTTP" if proxy == self.own_proxy else "HTTPS"
            self.logger.info(f"🔄 Trying with {proxy_type} {protocol} proxy: {proxy}")
            
            # Get headers from spider using get_headers_with_random_ua method
            headers = spider_instance.get_headers_with_random_ua() if hasattr(spider_instance, 'get_headers_with_random_ua') else {}
            
            # Create request with new proxy
            return scrapy.Request(
                url=orig_url,
                callback=request.callback,
                headers=headers,
                meta={
                    "row": request.meta.get('row'),
                    "proxy": proxy_url,
                    "proxies_list": proxies_list,
                    "proxy_index": proxy_index,
                    "product_rows": product_rows,
                    "product_index": product_index,
                    "dont_retry": True,
                    "handle_httpstatus_list": [403, 404, 429, 500, 502, 503],
                    "proxy_manager": self,
                    "spider_instance": spider_instance,
                    "url_id": url_id
                },
                errback=self.handle_error,
                dont_filter=True
            )
        else:
            # No more proxies, try to fetch new proxies
            self.logger.warning(f"⚠️ All proxies tried. Fetching new proxies for {orig_url}")
            
            # Fetch new proxy list
            return scrapy.Request(
                url=self.proxy_list_url,
                callback=self.parse_proxy_list_for_retry,
                meta={
                    "product_rows": product_rows,
                    "product_index": product_index,
                    "callback": request.callback,
                    "spider_instance": spider_instance,
                    "original_url": orig_url
                },
                dont_filter=True
            )

    def try_next_proxy(self, response):
        """Try the next proxy for the current product when validation pages or errors are detected"""
        request = response.request
        proxies_list = request.meta.get("proxies_list", [])
        proxy_index = request.meta.get("proxy_index", 0) + 1
        product_rows = request.meta.get("product_rows", [])
        product_index = request.meta.get("product_index", 0)
        callback = request.callback
        spider_instance = request.meta.get("spider_instance")
        
        # Log validation page detected
        self.logger.warning(f"⚠️ Validation page or error detected, trying next proxy")
        
        if proxy_index < len(proxies_list):
            # Try next proxy in current list
            proxy_info = proxies_list[proxy_index]
            proxy = proxy_info["proxy"]
            url = product_rows[product_index]["PriceLink"]
            
            # Use HTTP/HTTPS based on is_https flag
            is_https = proxy_info.get("is_https", False)
            proxy_url = f"https://{proxy}" if is_https else f"http://{proxy}"
            
            proxy_type = "own" if proxy == self.own_proxy else "public"
            protocol = "HTTPS" if is_https else "HTTP"
            self.logger.info(f"🔄 Switching to {proxy_type} {protocol} proxy: {proxy} ({proxy_index+1}/{len(proxies_list)})")
            
            # Get headers from spider using get_headers_with_random_ua method
            headers = spider_instance.get_headers_with_random_ua() if hasattr(spider_instance, 'get_headers_with_random_ua') else {}
            return scrapy.Request(
                url=url,
                callback=callback,
                headers=headers,
                meta={
                    "row": product_rows[product_index],
                    "proxy": proxy_url,
                    "proxies_list": proxies_list,
                    "proxy_index": proxy_index,
                    "product_rows": product_rows,
                    "product_index": product_index,
                    "dont_retry": True,
                    "handle_httpstatus_list": [403, 404, 429, 500, 502, 503],
                    "proxy_manager": self,
                    "spider_instance": spider_instance
                },
                errback=self.handle_error,
                dont_filter=True
            )
        else:
            # All proxies failed for this product, check if we've exceeded the maximum global refreshes
            if self.global_proxy_refresh_count >= self.max_global_proxy_refreshes:
                self.logger.error(f"❌ Maximum global proxy refreshes ({self.max_global_proxy_refreshes}) reached. Terminating.")
                # We've tried too many times globally, terminate processing
                return None
                
            # Otherwise, fetch new proxies
            self.logger.warning(f"⚠️ All {len(proxies_list)} proxies failed for this product, fetching new proxies...")
            
            # Request new proxy list from free-proxy-list.net
            # Get headers from spider using get_headers_with_random_ua method
            headers = spider_instance.get_headers_with_random_ua() if hasattr(spider_instance, 'get_headers_with_random_ua') else {}
            
            return scrapy.Request(
                url=self.proxy_list_url,
                callback=self.parse_proxy_list_fallback,
                meta={
                    'product_rows': product_rows,
                    'product_index': product_index,
                    'callback': callback,
                    'spider_instance': spider_instance,
                    'original_url': product_rows[product_index]["PriceLink"]
                },
                dont_filter=True
            )

    def parse_proxy_list_for_retry(self, response):
        """Parse proxy list page and retry the current product with new proxies"""
        product_rows = response.meta.get('product_rows', [])
        product_index = response.meta.get('product_index', 0)
        callback = response.meta.get('callback')
        spider_instance = response.meta.get('spider_instance')
        original_url = response.meta.get('original_url')
        
        # Increment global proxy refresh counter
        self.global_proxy_refresh_count += 1
        
        # Check if we've exceeded the maximum global refreshes
        if self.global_proxy_refresh_count > self.max_global_proxy_refreshes:
            self.logger.error(f"❌ Maximum global proxy refreshes ({self.max_global_proxy_refreshes}) reached. Terminating.")
            # We've tried too many times globally, terminate processing
            return None
        
        # Fetch new public proxies using our enhanced method
        public_proxies = self.fetch_proxies(response)
        
        # Add your own proxy at the beginning of the list (wrapped in dict for compatibility)
        own_proxy_dict = {"proxy": self.own_proxy, "is_https": False}  # Own proxy as HTTP
        
        # Limit proxies according to the limit_proxy_list setting
        public_proxies = public_proxies[:self.limit_proxy_list - 1]  # -1 for your own proxy
        proxies_list = [own_proxy_dict] + public_proxies
        
        self.logger.info(f"🔄 Fetched fresh proxies: your own proxy + {len(public_proxies)} public proxies (limited to {self.limit_proxy_list} total)")
        self.logger.info(f"Global proxy refresh count: {self.global_proxy_refresh_count}/{self.max_global_proxy_refreshes}")
        
        # If we don't have enough proxies, try fetching again after delay
        if len(proxies_list) < self.min_proxy_count:
            self.current_refresh_attempt += 1
            if self.current_refresh_attempt < self.max_proxy_refresh_attempts:
                self.logger.warning(f"⚠️ Found only {len(proxies_list)} proxies, less than minimum required ({self.min_proxy_count}). Attempt {self.current_refresh_attempt}/{self.max_proxy_refresh_attempts}")
                
                # Try requesting the proxy list again with a delay
                time.sleep(3)  # Wait 3 seconds before trying again
                
                # Using yield instead of return since this is a generator function
                yield scrapy.Request(
                    url='https://free-proxy-list.net/',
                    callback=self.parse_proxy_list_for_retry,
                    meta=response.meta,
                    dont_filter=True
                )
                return  # Stop execution after yielding the request
            else:
                self.logger.warning(f"⚠️ Max proxy refresh attempts ({self.max_proxy_refresh_attempts}) reached but still not enough proxies. Terminating.")
                # We don't have enough proxies and have tried too many times, terminate
                return None
        
        if not proxies_list:
            self.logger.error("❌ No proxies found even after refresh. Terminating.")
            return None
        
        # Get between_retries_delay from spider if available
        between_retries_delay = getattr(spider_instance, 'between_retries_delay', 5)
        
        # Add an additional delay before retrying
        if between_retries_delay > 0:
            self.logger.info(f"⏱️ Waiting {between_retries_delay} seconds before retrying with new proxies (from {spider_instance.name}'s settings)")
            time.sleep(between_retries_delay)
        
        # Start with first proxy from new list
        proxy_info = proxies_list[0]
        proxy = proxy_info["proxy"]
        
        # Use HTTP/HTTPS based on is_https flag
        is_https = proxy_info.get("is_https", False)
        proxy_url = f"https://{proxy}" if is_https else f"http://{proxy}"
        
        proxy_type = "own" if proxy == self.own_proxy else "public"
        protocol = "HTTPS" if is_https else "HTTP"
        self.logger.info(f"🔄 Retrying with fresh {proxy_type} {protocol} proxy: {proxy}")
        
        # Get headers from spider using get_headers_with_random_ua method
        headers = spider_instance.get_headers_with_random_ua() if hasattr(spider_instance, 'get_headers_with_random_ua') else {}
        # Add url_id for better tracking
        row = product_rows[product_index]
        url_id = row.get("BNCode", "unknown")
        
        yield scrapy.Request(
            url=original_url,
            callback=callback,
            headers=headers,
            meta={
                "row": row,
                "proxy": proxy_url,
                "proxies_list": proxies_list,
                "proxy_index": 0,
                "product_rows": product_rows,
                "product_index": product_index,
                "dont_retry": True,
                "handle_httpstatus_list": [403, 404, 429, 500, 502, 503],
                "proxy_manager": self,
                "spider_instance": spider_instance,
                "url_id": url_id
            },
            errback=self.handle_error,
            dont_filter=True
        )

    def add_human_delay(self, spider_instance):
        """Add a human-like delay if the spider has use_human_like_delay=True"""
        use_human_like_delay = getattr(spider_instance, 'use_human_like_delay', False)
        
        if use_human_like_delay:
            # Get spider-specific sleep time settings if available
            min_sleep_time = getattr(spider_instance, 'min_sleep_time', 1)
            max_sleep_time = getattr(spider_instance, 'max_sleep_time', 4)
            
            # Add a random sleep time between min and max
            sleep_time = random.uniform(min_sleep_time, max_sleep_time)
            
            # Log the source of the delay settings
            if hasattr(spider_instance, 'min_sleep_time') and hasattr(spider_instance, 'max_sleep_time'):
                self.logger.info(f"😴 Human-like delay: Sleeping for {sleep_time:.2f} seconds (using {spider_instance.name}'s range: {min_sleep_time}-{max_sleep_time}s)")
            else:
                self.logger.info(f"😴 Human-like delay: Sleeping for {sleep_time:.2f} seconds (using default range: 1-4s)")
                
            time.sleep(sleep_time)
            return True
        return False

    def process_next_product(self, response, next_product_index):
        """Process the next product using the same proxy that worked for the previous product"""
        proxies_list = response.meta.get("proxies_list", [])
        proxy_index = response.meta.get("proxy_index", 0)
        product_rows = response.meta.get("product_rows", [])
        callback = response.request.callback
        spider_instance = response.meta.get("spider_instance")
        
        if next_product_index < len(product_rows):
            # Get between_products_delay from spider if available
            between_products_delay = getattr(spider_instance, 'between_products_delay', 1)
            
            # Add delay between products if specified
            if between_products_delay > 0:
                self.logger.info(f"⏱️ Waiting {between_products_delay} seconds between products (from {spider_instance.name}'s settings)")
                time.sleep(between_products_delay)
            
            # Add human-like delay if enabled
            self.add_human_delay(spider_instance)
            
            # Get proxy that worked for the current product
            proxy_info = proxies_list[proxy_index]
            proxy = proxy_info["proxy"]
            url = product_rows[next_product_index]["PriceLink"]
            
            # Use HTTP for your own proxy, HTTPS for all public proxies
            proxy_url = f"http://{proxy}" if proxy == self.own_proxy else f"https://{proxy}"
            
            # Log the progress
            remaining = len(product_rows) - next_product_index
            proxy_type = "own" if proxy == self.own_proxy else "public"
            protocol = "HTTP" if proxy == self.own_proxy else "HTTPS"
            self.logger.info(f"⏭️ Moving to next product ({next_product_index+1}/{len(product_rows)}) | Remaining: {remaining} | Using working {proxy_type} {protocol} proxy: {proxy}")
            
            # Get headers from spider using get_headers_with_random_ua method
            headers = spider_instance.get_headers_with_random_ua() if hasattr(spider_instance, 'get_headers_with_random_ua') else {}
            # Add url_id for better tracking
            url_id = product_rows[next_product_index].get("BNCode", "unknown")
            
            return scrapy.Request(
                url=url,
                callback=callback,
                headers=headers,
                meta={
                    "row": product_rows[next_product_index],
                    "proxy": proxy_url,
                    "proxies_list": proxies_list,
                    "proxy_index": proxy_index,
                    "product_rows": product_rows,
                    "product_index": next_product_index,
                    "dont_retry": True,
                    "handle_httpstatus_list": [403, 404, 429, 500, 502, 503],
                    "proxy_manager": self,
                    "spider_instance": spider_instance,
                    "url_id": url_id
                },
                errback=self.handle_error,
                dont_filter=True
            )
        else:
            self.logger.info("✅ All products processed!")
            return None

    def handle_proxy_fetch_error(self, failure):
        """Handle cases where the proxy list site is unreachable - fall back to direct requests"""
        # Extract relevant information from failure meta
        request = failure.request
        product_rows = request.meta.get('product_rows', [])
        callback = request.meta.get('callback')
        spider_instance = request.meta.get('spider_instance')
        
        # Log the error
        error_type = type(failure.value).__name__
        error_msg = str(failure.value)
        self.logger.error(f"❌ Error fetching proxy list: {error_type} - {error_msg}")
        self.logger.info(f"⚠️ Proceeding with direct requests (without proxies)")
        
        # Process the first few products directly
        results = []
        for i, row in enumerate(product_rows[:5]):  # Start with first 5 products
            # Add a small delay between requests
            time.sleep(0.5)
            
            url = row["PriceLink"]
            url_id = row.get("BNCode", "unknown")
            
            # Get headers with a random user agent
            headers = spider_instance.get_headers_with_random_ua() if hasattr(spider_instance, 'get_headers_with_random_ua') else {}
            
            # Prepare meta dictionary
            meta = {
                "row": row,
                "product_rows": product_rows,
                "product_index": i,
                "url_id": url_id,
                "direct_mode": True  # Flag to indicate we're in direct mode
            }
            
            self.logger.info(f"🚀 Sending direct request {i+1}/{len(product_rows)} for {url_id}")
            results.append(
                scrapy.Request(
                    url=url,
                    callback=callback,
                    headers=headers,
                    meta=meta,
                    dont_filter=True
                )
            )
        
        return results

    def parse_proxy_list_fallback(self, response):
        """Fallback method to parse free-proxy-list.net"""
        self.logger.info("🔍 Parsing proxies from free-proxy-list.net")
        
        # Increment global proxy refresh counter
        self.global_proxy_refresh_count += 1
        
        # Check if we've exceeded the maximum global refreshes
        if self.global_proxy_refresh_count > self.max_global_proxy_refreshes:
            self.logger.error(f"❌ Maximum global proxy refreshes ({self.max_global_proxy_refreshes}) reached. Terminating.")
            # We've tried too many times globally, terminate processing
            return None
        
        # Get public proxies using the fetch_proxies method
        public_proxies = self.fetch_proxies(response)
        
        # Add your own proxy at the beginning of the list (wrapped in dict for compatibility)
        own_proxy_dict = {"proxy": self.own_proxy, "is_https": False}  # Own proxy as HTTP
        
        # Limit proxies according to the limit_proxy_list setting
        public_proxies = public_proxies[:self.limit_proxy_list - 1]  # -1 for your own proxy
        proxies_list = [own_proxy_dict] + public_proxies
        
        # Log number of proxies found
        self.logger.info(f"Found {len(proxies_list)} proxies from free-proxy-list.net (limited to {self.limit_proxy_list} total)")
        self.logger.info(f"Global proxy refresh count: {self.global_proxy_refresh_count}/{self.max_global_proxy_refreshes}")
        
        # If we don't have enough proxies, try fetching again after delay
        if len(proxies_list) < self.min_proxy_count:
            self.current_refresh_attempt += 1
            if self.current_refresh_attempt < self.max_proxy_refresh_attempts:
                self.logger.warning(f"⚠️ Found only {len(proxies_list)} proxies, less than minimum required ({self.min_proxy_count}). Attempt {self.current_refresh_attempt}/{self.max_proxy_refresh_attempts}")
                
                # Try requesting the proxy list again with a delay
                time.sleep(3)  # Wait 3 seconds before trying again
                
                # Using yield instead of return since this is a generator function
                yield scrapy.Request(
                    url='https://free-proxy-list.net/',
                    callback=self.parse_proxy_list_fallback,
                    meta=response.meta,
                    dont_filter=True
                )
                return  # Stop execution after yielding the request
            else:
                self.logger.warning(f"⚠️ Max proxy refresh attempts ({self.max_proxy_refresh_attempts}) reached but still not enough proxies. Terminating.")
                # We don't have enough proxies and have tried too many times, terminate
                return None
        
        if not proxies_list:
            self.logger.error("❌ No proxies found even after refresh. Terminating.")
            return None
            
        meta = response.meta
        product_rows = meta.get('product_rows', [])
        product_index = meta.get('product_index', 0)
        callback = meta.get('callback')
        spider_instance = meta.get('spider_instance')
        original_url = meta.get('original_url')
        
        # Get spider headers
        headers = spider_instance.get_headers_with_random_ua() if hasattr(spider_instance, 'get_headers_with_random_ua') else {}
        
        # Create a new request with the first proxy
        if proxies_list:
            proxy_info = proxies_list[0]
            proxy = proxy_info["proxy"]
            
            # Use HTTP/HTTPS based on is_https flag
            is_https = proxy_info.get("is_https", False)
            proxy_url = f"https://{proxy}" if is_https else f"http://{proxy}"
            
            proxy_type = "own" if proxy == self.own_proxy else "public"
            protocol = "HTTPS" if is_https else "HTTP"
            self.logger.info(f"🔄 Continuing with {proxy_type} {protocol} proxy: {proxy} for {original_url}")
            
            yield scrapy.Request(
                url=original_url,
                callback=callback,
                headers=headers,
                meta={
                    "row": product_rows[product_index],
                    "proxy": proxy_url,
                    "proxies_list": proxies_list,
                    "proxy_index": 0,
                    "product_rows": product_rows,
                    "product_index": product_index,
                    "dont_retry": True,
                    "handle_httpstatus_list": [403, 404, 429, 500, 502, 503],
                    "proxy_manager": self,
                    "spider_instance": spider_instance
                },
                errback=self.handle_error,
                dont_filter=True
            )
        else:
            self.logger.error("⚠️ No proxies found from free-proxy-list.net, skipping product")
            
            # Move to next product if there are any left
            if product_index + 1 < len(product_rows):
                return self.start_proxy_fetch(
                    callback=callback,
                    spider_instance=spider_instance,
                    product_rows=product_rows,
                    start_index=product_index + 1
                )
            return None
            
    # Add a method to facilitate integration with spiders
    def get_start_request(self, callback, spider_instance, product_rows):
        """Helper method to get a single Request to start the crawling process"""
        return self.start_proxy_fetch(callback, spider_instance, product_rows)


