from src.spiders.base_spider import BaseSpider
from src.items import ProductItem
from src.utils.proxy_manager import ProxyManagerFreeProxyListDotNet
from src.utils.price_formatter import format_pl_price
import scrapy
import logging
import json
import random
import time
import re
import urllib.parse

class Neonet(BaseSpider):
    name = "neonet"
    market_player = "Neonet"
    use_proxy_manager = True
    use_human_like_delay = True
    # Custom sleep time range for human-like delays
    min_sleep_time = 5  # Minimum sleep time in seconds
    max_sleep_time = 10  # Maximum sleep time in seconds

    custom_settings = {
        **BaseSpider.custom_settings,
        "RANDOMIZE_DOWNLOAD_DELAY": True,  # Randomize delay by 0.5-1.5x
        'HTTPERROR_ALLOWED_CODES': [202, 404, 410, 429, 503],
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
        """
        Check if the response is a validation page or an error page.
        Returns True if it detects a validation page, False otherwise.
        """
        # Check for HTTP headers dump in the response (common in anti-bot responses)
        headers_dump = any(header in response.text.lower() for header in [
            'user-agent:', 'accept-encoding:', 'accept-language:', 'referer:'
        ])
        if headers_dump:
            self.log_debug("Found HTTP headers dump in the response - likely validation page")
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
                callback=self.parse_id,
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
                    callback=self.parse_id,
                    headers=headers,
                    meta=meta,
                    dont_filter=True
                )
                
    def parse_id(self, response):
        """Extract product ID from the page and make a GraphQL request"""
        # Handle validation pages or errors
        if self.use_proxy_manager:
            proxy_manager = response.meta.get("proxy_manager", self.proxy_manager)
            
            if (response.status != 200 and response.status not in self.allowed_error_codes) or self.is_validation_page(response):
                self.log_info("Validation page or error detected - trying next proxy")
                next_request = proxy_manager.try_next_proxy(response)
                if next_request:
                    yield next_request
                    return  # Return without value after yielding
                return
        else:
            # Without proxy manager, just skip invalid responses
            if (response.status != 200 and response.status not in self.allowed_error_codes) or self.is_validation_page(response):
                self.log_error(f"⚠️ Invalid response for {response.url}")
                return
        
        # Extract metadata
        row = response.meta["row"]
        product_rows = response.meta.get("product_rows", [])
        product_index = response.meta.get("product_index", 0)
        price_link = row["PriceLink"]
        url_id = row.get("BNCode", "unknown")
        
        # Extract product ID using regex
        script_content = response.xpath("//script[contains(text(), 'urlResolver')]/text()").get()
        if not script_content:
            self.log_error(f"⚠️ Cannot find product script for {response.url}")
            
            if response.status == 404:
                # Create product item with default values for missing product
                item = ProductItem()
                item["price_link"] = price_link
                item["xpath_result"] = "0.00"
                item["out_of_stock"] = "Outstock"
                item["market_player"] = self.market_player
                if "BNCode" in row:
                    item["bn_code"] = row["BNCode"]
                
                self.log_info(f"Setting default values for 404 {url_id}: price=0.00, status=Outstock")
                yield item
            # Move to next product if using proxy manager
            if self.use_proxy_manager:
                next_product_index = product_index + 1
                next_request = proxy_manager.process_next_product(response, next_product_index)
                if next_request:
                    next_request = next_request.replace(callback=self.parse_id)
                    yield next_request
            return
            
        product_id_match = re.search(r'id: "(\d+)"', script_content)
        if not product_id_match:
            self.log_error(f"⚠️ Cannot extract product ID for {response.url}")
            
            # Create product item with default values for missing product ID
            item = ProductItem()
            item["price_link"] = price_link
            item["xpath_result"] = "0.00"
            item["out_of_stock"] = "Outstock"
            item["market_player"] = self.market_player
            if "BNCode" in row:
                item["bn_code"] = row["BNCode"]
            
            self.log_info(f"Setting default values for {url_id}: price=0.00, status=Outstock")
            yield item
            
            # Move to next product if using proxy manager
            if self.use_proxy_manager:
                next_product_index = product_index + 1
                next_request = proxy_manager.process_next_product(response, next_product_index)
                if next_request:
                    next_request = next_request.replace(callback=self.parse_id)
                    yield next_request
            return
            
        product_id = product_id_match.group(1)
        self.log_info(f"Found product ID: {product_id} for {url_id}")
        
        # Prepare GraphQL request
        graphql_url = 'https://www.neonet.pl/graphql'
        
        # Prepare the GraphQL query for product data
        query = '''
        query msProducts( $ids: [Int] ) {
            products: msProducts(
                filter: { skus: $ids }
                attributes: true
            ) {
                items {
                    id,sku,neonet_product_id,type_id,availability,availability_status_element,
                    name,price,final_price,
                    is_product_available,
                    autopromotion(lp_promotion:false){rule_id,promo_code,discount,discount_type,show_in_listing,show_on_product,date_to,show_on_homepage,show_qty_sold,show_qty_not_sold,show_time_to_end,shock_header,price,date_from,stock_used,stock_left,custom_shockprice_header,promo_code,is_hidden,is_shockprice,tooltip_text,hide_omnibus_price},
                }
            }
        }
        '''
        
        # Clean up the query by removing whitespace
        query = ' '.join(query.split())
        
        # URL encode the query
        encoded_query = urllib.parse.quote(query)
        
        # Create the full GraphQL URL with variables
        full_url = f'{graphql_url}?query={encoded_query}&variables=%7B%22ids%22:%5B{product_id}%5D%7D&v=2.221.0'
        
        # Create headers with referer
        headers = self.get_headers_with_random_ua()
        headers.update({
            'sec-ch-ua-platform': '"Windows"',
            'Referer': response.url,
            'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            'content-type': 'application/json',
            'sec-ch-ua-mobile': '?0',
        })
        
        # Pass all the original meta data
        meta = response.meta.copy()
        meta['product_id'] = product_id
        meta['original_url'] = response.url
        
        # Make the GraphQL request
        yield scrapy.Request(
            url=full_url,
            callback=self.parse_graphql,
            errback=self.handle_graphql_error,  # Add an errback handler for timeout errors
            headers=headers,
            meta=meta,
            dont_filter=True
        )
        
    def handle_graphql_error(self, failure):
        """Handle errors that occur during GraphQL requests, especially timeouts"""
        # Get the original request
        request = failure.request
        meta = request.meta
        proxy_manager = meta.get("proxy_manager", self.proxy_manager)
        row = meta.get("row")
        product_rows = meta.get("product_rows", [])
        product_index = meta.get("product_index", 0)
        original_url = meta.get("original_url")
        url_id = row.get("BNCode", "unknown") if row else "unknown"
        
        # Log the error
        error_type = type(failure.value).__name__
        self.log_error(f"⚠️ GraphQL request error ({error_type}) for {url_id}: {str(failure.value)}")
        
        # Return an item with default values
        if row:
            item = ProductItem()
            item["price_link"] = row.get("PriceLink", "")
            item["xpath_result"] = "0.00"
            item["out_of_stock"] = "Outstock"
            item["market_player"] = self.market_player
            if "BNCode" in row:
                item["bn_code"] = row["BNCode"]
                
            self.log_info(f"Setting default values for {url_id}: price=0.00, status=Outstock (GraphQL error)")
            yield item
            
            # Move to the next product if using proxy manager
            if self.use_proxy_manager:
                next_product_index = product_index + 1
                if product_rows and next_product_index < len(product_rows):
                    next_request = proxy_manager.process_next_product(request, next_product_index)
                    if next_request:
                        # Override the callback to ensure we always start with parse_id for a new product
                        next_request = next_request.replace(callback=self.parse_id)
                        yield next_request

    def parse_graphql(self, response):
        """Parse the GraphQL API response to extract price and stock information"""
        # Handle validation pages or errors
        if self.use_proxy_manager:
            proxy_manager = response.meta.get("proxy_manager", self.proxy_manager)
            
            if (response.status != 200 and response.status not in self.allowed_error_codes) or self.is_validation_page(response):
                self.log_info("GraphQL response invalid - trying next proxy")
                next_request = proxy_manager.try_next_proxy(response)
                if next_request:
                    original_url = response.meta.get('original_url')
                    if original_url:
                        next_request = next_request.replace(url=original_url, callback=self.parse_id)
                    yield next_request
                    return  # Return without value after yielding
                return
        else:
            # Without proxy manager, just skip invalid responses
            if (response.status != 200 and response.status not in self.allowed_error_codes) or self.is_validation_page(response):
                self.log_error(f"⚠️ Invalid response for {response.url}")
                return
        
        # Extract metadata
        row = response.meta["row"]
        product_rows = response.meta.get("product_rows", [])
        product_index = response.meta.get("product_index", 0)
        price_link = row["PriceLink"]
        url_id = row.get("BNCode", "unknown")
        product_id = response.meta.get("product_id", "unknown")
        original_url = response.meta.get("original_url")
        
        try:
            price = ""
            stock_status = ""
            
            # Check if response is valid JSON
            if not response.text.strip().startswith('{'):
                self.log_error(f"⚠️ Response is not valid JSON: {response.text[:30]}")
                
                # Create product item with default values for invalid JSON
                item = ProductItem()
                item["price_link"] = price_link
                item["xpath_result"] = "0.00"
                item["out_of_stock"] = "Outstock"
                item["market_player"] = self.market_player
                if "BNCode" in row:
                    item["bn_code"] = url_id
                
                self.log_info(f"Setting default values for {url_id}: price=0.00, status=Outstock (invalid JSON)")
                yield item
                
                # Move to next product if using proxy manager
                if self.use_proxy_manager:
                    next_product_index = product_index + 1
                    next_request = proxy_manager.process_next_product(response, next_product_index)
                    if next_request:
                        next_request = next_request.replace(callback=self.parse_id)
                        yield next_request
                return
            
            # Parse JSON response
            data = json.loads(response.text)
            
            if 'data' in data:
                # Check if we have product data
                if 'products' in data['data'] and 'items' in data['data']['products'] and data['data']['products']['items']:
                    product = data['data']['products']['items'][0]
                    print(data)
                    discount = 'autopromotion' in product and 'discount' in product['autopromotion'] and product['autopromotion']['discount'] or 0
                    # Extract price
                    if 'final_price' in product:
                        price = format_pl_price(str(product['final_price'] - discount))
                    elif 'price' in product:
                        price = format_pl_price(str(product['price'] - discount))
                    
                    # Extract stock status
                    if 'is_product_available' in product:
                        stock_status = "Instock" if product['is_product_available'] else "Outstock"
                    
                    # If we determine it's out of stock, set price to 0
                    if stock_status == "Outstock":
                        price = "0.00"
                    
                    # If we have a price but no stock status, assume it's in stock
                    if price and not stock_status:
                        stock_status = "Instock"
                    
                    # If we have no price, assume it's out of stock
                    if not price:
                        stock_status = "Outstock"
                        price = "0.00"
                else:
                    stock_status = "Outstock"
                    price = "0.00"
            else:
                self.log_error(f"⚠️ No product data found in GraphQL response for {product_id}")
                
                # Create product item with default values for missing GraphQL data
                item = ProductItem()
                item["price_link"] = price_link
                item["xpath_result"] = "0.00"
                item["out_of_stock"] = "Outstock"
                item["market_player"] = self.market_player
                if "BNCode" in row:
                    item["bn_code"] = url_id
                
                self.log_info(f"Setting default values for {url_id}: price=0.00, status=Outstock (no GraphQL data)")
                yield item
                
                # Move to next product if using proxy manager
                if self.use_proxy_manager:
                    next_product_index = product_index + 1
                    next_request = proxy_manager.process_next_product(response, next_product_index)
                    if next_request:
                        next_request = next_request.replace(callback=self.parse_id)
                        yield next_request
                return
                
            remaining = len(product_rows) - (product_index + 1)
            self.log_info(f"[{product_index+1}/{len(product_rows)}] {url_id}: {price} PLN | {stock_status} | {remaining} remaining")
                
        except Exception as e:
            self.log_error(f"⚠️ Error processing GraphQL response for {url_id}: {str(e)}")
            
            # Create product item with default values for JSON processing error
            item = ProductItem()
            item["price_link"] = price_link
            item["xpath_result"] = "0.00"
            item["out_of_stock"] = "Outstock"
            item["market_player"] = self.market_player
            if "BNCode" in row:
                item["bn_code"] = url_id
            
            self.log_info(f"Setting default values for {url_id}: price=0.00, status=Outstock (JSON error: {str(e)[:30]})")
            yield item
            
            # Move to next product if using proxy manager
            if self.use_proxy_manager:
                next_product_index = product_index + 1
                next_request = proxy_manager.process_next_product(response, next_product_index)
                if next_request:
                    next_request = next_request.replace(callback=self.parse_id)
                    yield next_request
            return
        
        # Create product item
        item = ProductItem()
        item["price_link"] = price_link
        item["xpath_result"] = price
        item["out_of_stock"] = stock_status
        item["market_player"] = self.market_player
        if "BNCode" in row:
            item["bn_code"] = row["BNCode"]
        yield item
        
        # Move to next product - IMPORTANT FIX: Always use parse_id callback for the next product
        if self.use_proxy_manager:
            next_product_index = product_index + 1
            next_request = proxy_manager.process_next_product(response, next_product_index)
            if next_request:
                # Override the callback to ensure we always start with parse_id for a new product
                next_request = next_request.replace(callback=self.parse_id)
                yield next_request
    