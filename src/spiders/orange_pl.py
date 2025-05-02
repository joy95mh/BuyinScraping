from src.spiders.base_spider import BaseSpider
from src.items import ProductItem
from src.utils.proxy_manager import ProxyManagerFreeProxyListDotNet
from src.utils.price_formatter import format_pl_price
import scrapy
import logging
import json
import random
import time

class Orange(BaseSpider):
    name = "orange"
    market_player = "Orange"
    use_proxy_manager = False
    use_human_like_delay = True
    
    # Custom sleep time range for human-like delays
    min_sleep_time = 25  # Minimum sleep time in seconds
    max_sleep_time = 35  # Maximum sleep time in seconds
    between_products_delay = 3  # Additional delay between products when using proxy manager
    between_retries_delay = 1  # Delay before retrying with a new proxy after failure
    
    custom_settings = {
        **BaseSpider.custom_settings,
        'DOWNLOAD_DELAY': 5,
        'HTTPERROR_ALLOW_ALL': False,
        'HTTPERROR_ALLOWED_CODES': [404, 410, 429, 503],  # Allow 429 status code
        'DOWNLOAD_TIMEOUT': 15,  # Increase timeout
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
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
        # 'cookie': 'opl_feat_enableNbaRecommendationsUpsell=1; opl_feat_enableNbaRecommendationsUpsell_source=100-20.03.2025; opl_feat_enableMVOfferConfigurator=1; opl_feat_enableMVOfferConfigurator_source=100-16.03.2025-17.02.2025; opl_feat_gocartCheckoutCartFixLove=0; opl_feat_gocartCheckoutCartFixLove_source=2-08.04.2025-08.04.2025; ABTest_b2b_cookie_tartu=1; ABTest_NetflixBanner_cookie=1; ABTest_simpli_ol_page_ab_test=1; ABTest_main_b2b_page_ab_test=1; ABTest_simpli_fix_page_ab_test=1; opl_feat_test5=0; opl_feat_test10=0; opl_feat_test20=0; opl_feat_test25=0; opl_feat_test50=0; genesys.tracker.globalVisitID=340942b3-9e9f-4b09-b8c3-65e2055b664a; didomi_token=eyJ1c2VyX2lkIjoiMTk2M2VmZWMtNjllZS02OTM1LWI1NzItZGEyMTUyZDM4ZWMyIiwiY3JlYXRlZCI6IjIwMjUtMDQtMTZUMTQ6MjY6NDYuNTU4WiIsInVwZGF0ZWQiOiIyMDI1LTA0LTE2VDE0OjI3OjM0LjA2MloiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwiYzpnb29nbGVhbmEtNFRYbkppZ1IiLCJjOnNuYXBjaGF0YS1oQnRpelllSiIsImM6dGlrdG9rYWQtbmJuVmdFN1UiLCJjOmFkZm9ybS1xeWFHWlRSMyIsImM6Y3JpdGVvLUxjV3I2QnFKIiwiYzpydGJob3VzZS15bmlRZDNIRiIsImM6eW91cmN4LTNkNHpiWFFuIiwiYzpsaW5rZWRpbi1hSHFFa3RrNiIsImM6c3luZXJpc2UtYWhHNmtmQmQiLCJjOnF1YW50dW1tZS1paFVMQXdHWCIsImM6ZmFjZWJvb2tmLXppcEVUQU5UIl19LCJwdXJwb3NlcyI6eyJlbmFibGVkIjpbImRpc3BsYXlpbmctWmRGRlFxZ3oiLCJjcmVhdGluZ2EtSGk2aE5MZWIiLCJwZXJmb3JtaW5nLUh4MmVNN1hWIiwidHJhY2tpbmdhLTlNZ2F0QzZEIiwicGVyc29uYWxpemVkX2NvbnRlbnQiLCJjcmVhdGluZ2gtcHdublIzRWkiLCJyZWNvcmRpbmctZ3hlekRkWngiXX0sInZlbmRvcnNfbGkiOnsiZW5hYmxlZCI6WyJnb29nbGUiLCJjOmJpbmtpZXMzLVh3cEJ5WG1aIl19LCJwdXJwb3Nlc19saSI6eyJlbmFibGVkIjpbInBlcmZvcm1pbmctSHgyZU03WFYiXX0sInZlcnNpb24iOjIsImFjIjoiQWVhQUNBZVkuQWVhQUNBZVkifQ==; euconsent-v2=CQP9X4AQP9X4AAHABBENBlFgAPNAAELAAB5YF5wAQF5gXnABAXmAAAAA.fmgACFgAAAAA; _ga=GA1.1.480114097.1744813611; _gcl_au=1.1.1471748037.1744813654; _fbp=fb.1.1744813656646.881779873428078267; QuantumMetricUserID=60a1a85a6202ab2286b99eecad9feeef; _snrs_uuid=8d739612-49d1-4c9c-931c-fea2d33ba792; _snrs_puuid=8d739612-49d1-4c9c-931c-fea2d33ba792; ftb2b=A1; opl_feat_marketSelection=0; opl_feat_enableNsg=1; opl_feat_enableNsg_source=100-17.04.2025-17.03.2025; ftfixnet=B; ft40b=11; dtCookie=v_4_srv_8_sn_2AF50E67B0DC400FB1515CD46B5AF070_perc_100000_ol_0_mul_1_app-3Aa338c7a7bdfbfa55_0; USID=82cca5dc62ff93ab0586458c5377ceeb; logtrackerID=82cca5dc62ff93ab0586458c5377ceeb; master-process-variant=master_v2; opl-portal-master-process=5105dd7b-41ce-490c-b796-74c63f7dea99-f9893c96-dd4c-47bb-99f3-0025224fede8; hybsessionid=00EEF48A9400164704DB72CE172489BF.hyb17; LoginToken="1:KEesqD1sHv/ccpySVZBE2g==V4X+rpVTo1bOsg/lOVdLlL1cLcJJiBEFYUNlOI84/RXioCxGj+FbLDq5qWLCfWSDziyBmldKMtrn HqDo8GuHkA=="; firstvisitsessionid=s24093327401364; ABTest_wideorec_page_ab_test=1; ABTest_wideoprez_page_ab_test=0; HID=hyb17; TS0105f6b2=01b0228c7542fbdc9cb7eb0b46fbbf51022177149d834b61e2965170bb8ad287ce78ec46dc49569c17be805b4e88c02f577167e1b4; QuantumMetricSessionID=407c9ab6e05d6fbc30c5ad399b825cc3; TS00000000076=08cb46268eab28006698ab770284b21dc6e57f9b608100cd1142be7bd27b9f4202a79a81ed6e13aafa9d2b24b9e830090886fb747d09d0002f8cc61ba588599e33e7b93d2189ff487d0e96d35ecdcb7856eeb599859a440d02b4b0e93eecce15cb73c2243485eb21cb5c3a0eb2ef48fb875c271b5bedc17e64e183e4e9b5f556f06a391041d83fe367fb3163a62c7557a63db561dfb4bec7b9861dc8cd6987d779bf6aecb3d95aa03a4b1703cd9846041380db4c63e363e71faaa50544fbfac61a2fb39c3f4a735bfdddebf7173e7d0506fd53fafa4bedf2a20f70e1a79852b74cc94f53f942c045e5222c9c82db7f74eaf39865fab76f09365219b639b149d9f81ff100839c729e; TSPD_101_DID=08cb46268eab28006698ab770284b21dc6e57f9b608100cd1142be7bd27b9f4202a79a81ed6e13aafa9d2b24b9e830090886fb747d063800a36f3921fddac81c38248c021eccc61be7d016cbd7e460e00776ca68400ec95302f03ce81668489d3afb41164ddc1c7fc248cb447a993e03; genesys.tracker.serverAlias=.PT_DMZ_GWE_N1; genesys.tracker.visitID=381a3b02-70bb-45a3-b928-8261331d1cfd; TSPD_101=08cb46268eab2800eee61f543bb068d310910da8258fa9d1db777a56afdc7bf6c5c1207003c67b33062e3bdbc4a4354008483aa9f905180098c1f09f91b52c4f09c1896554fb572ff79a3fc2976edb0d; __rtbh.uid=%7B%22eventType%22%3A%22uid%22%2C%22id%22%3A%22unknown%22%2C%22expiryDate%22%3A%222026-04-18T15%3A59%3A25.475Z%22%7D; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22rzPdzndhzup1beOuZjok%22%2C%22expiryDate%22%3A%222026-04-18T15%3A59%3A25.475Z%22%7D; _snrs_sa=ssuid:4f3d77f2-8174-4c90-80c8-d75f0f46a1d0&appear:1744991953&sessionVisits:3; _snrs_sb=ssuid:4f3d77f2-8174-4c90-80c8-d75f0f46a1d0&leaves:1744992219; _snrs_p=host:www.orange.pl&permUuid:8d739612-49d1-4c9c-931c-fea2d33ba792&uuid:8d739612-49d1-4c9c-931c-fea2d33ba792&identityHash:&user_hash:&init:1744813657&last:1744991954.016&current:1744992219&uniqueVisits:2&allVisits:7&globalControlGroup:false; _ga_9LJDC1964E=GS1.1.1744991947.2.1.1744992234.30.0.0; TSd77532ef077=08cb46268eab28000d1a7b687d44b049c0d2e05aa208222e180d64f889f97a3dda365716ad4bb820d5f5d4ac1864ebd9080ede9a03172000d6e115cbfcb84f29f218929ba96d4f8c7bb87fdd58a82b95d56faf48c1ac0229; TS01817873=01b0228c75f4a77cb90a2c4a9308cf7e7292fed6654edb30ed5bd5663cb8a5bd9c7a1fff1261e636e69a8841437cec7b472942f6748878410f00c592325e8c74ba2bbdf8d78ebb6db14073e90d97088d947658c96ee69c3c2b133ae7f3fc936970744d555a9cf2334c982318ebe073d0e868a74590d34366a8973d66f5d4da053503dcf197d10b6ffa626f1ed0dbfc45302774bd3c40979b8fee6bf5009f587c4d84a85d73623bfd57b3d88ac33fc8e2644d2baf37e49629f8671fbd1e5e2237eee6928a557fe56d7893c0a73d4460971d6147b62ecbd295aabd482f5408d6d4d24ab8c37b4192fa84be56978bec9c89b27e95e2c5fde03f97d9c90f50b846866c42cc8d01a072a59d3850ec92315df4cd3dc75273fcdc50c0df3a8cb13fd55e7fa3c185a87038e5a9f7674f3f0287674cdf39a2028aad89b884a22921e1a4a639ef0312e4a0ffb56135742d64583c5868c0fbd8eef20dbc85bed4d849b4b5ea45ab54cf7615213935bd6d6223e9e86e7091c7854bebd8af8e9663228d42c9ee562e3e7bb9; TS4e0aa58f027=08cb46268eab2000b3ecc1ebf63d9b14f75ef5c3a5160c859fcb79c0ab361a51442dc1f4a2d1129e08e7141e961130004e4a10918c634e78e0a10b00b61e55ecc3dfb241023d1e716a1104eaf44294df797b1c9f87043afaa96e21ec6f3b5c1d',
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
                
                # self.log_info(f"🔍 Preparing request {i+1}/{len(product_rows)} | ID: {url_id}")
                
                # Add a random sleep time if human-like delay is enabled - use longer delays for Komputronik
                if self.use_human_like_delay:
                    sleep_time = random.uniform(self.min_sleep_time, self.max_sleep_time)
                    self.log_info(f"😴 Sleeping for {sleep_time:.2f} seconds before request {i+1}/{len(product_rows)}")
                    time.sleep(sleep_time)
                
                # Get headers with a random user agent
                headers = self.get_headers_with_random_ua()
                
                # If we have a proxy manager but it's disabled, use the own_proxy directly
                meta = {
                    "row": row,
                    "product_rows": product_rows,
                    "product_index": i,
                    "handle_httpstatus_list": [404, 410, 429, 503],  # Add 429 to handled status codes
                    "retry_count": 0,  # Track retries
                    "url_id": url_id
                }
                
                self.log_info(f"🚀 Sending request {i+1}/{len(product_rows)} to {url} for {url_id}")
                
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    headers=headers,
                    meta=meta,
                    dont_filter=True
                )

    def parse(self, response):
        row = response.meta["row"]
        product_rows = response.meta.get("product_rows", [])
        product_index = response.meta.get("product_index", 0)
        price_link = row["PriceLink"]
        url_id = response.meta.get("url_id", row.get("BNCode", "unknown"))
        retry_count = response.meta.get("retry_count", 0)
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
        # Log the response status
        self.log_info(f"📋 Response for {url_id} | Status: {response.status} | Size: {len(response.body)} bytes")
        
        # Handle 429 Too Many Requests - implement exponential backoff
        if response.status == 429:
            # Max retries to prevent infinite loops
            if retry_count >= 3:
                self.log_error(f"⚠️ Max retries exceeded for {url_id} after 429 status code")
                # Return product as out of stock when we can't process it
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
            backoff_time = 60 * (2 ** retry_count)  # 60s, 120s, 240s
            self.log_warning(f"⚠️ Rate limited (429) for {url_id}. Retry {retry_count+1}/3 after {backoff_time} seconds")
            
            # Schedule a retry with increased delay
            meta = response.meta.copy()
            meta["retry_count"] = retry_count + 1
            
            # Get fresh headers with a new random user agent
            headers = self.get_headers_with_random_ua()
            
            # Use time.sleep for the backoff (or you could use twisted's reactor.callLater in production)
            time.sleep(backoff_time)
            
            yield scrapy.Request(
                url=response.url,
                callback=self.parse,
                headers=headers,
                meta=meta,
                dont_filter=True
            )
            return
        
        # For other error responses or validation pages
        if response.status != 200 or self.is_validation_page(response):
            proxy_manager = response.meta.get("proxy_manager", self.proxy_manager)
            if self.use_proxy_manager and proxy_manager:
                # Use yield from to properly handle the generator from try_next_proxy
                for request in proxy_manager.try_next_proxy(response):
                    yield request
                return  # Important: return after yielding from generator
            else:
                self.log_error(f"⚠️ Error or validation page detected but no proxy manager available: Status {response.status}")
                return

        try:
            price = ""
            stock_status = ""

            # Try to get price from JSON-LD data first
            data_text = response.xpath("//script[@type='application/ld+json' and contains(text(),'priceCurrency')]/text()").get()
            if data_text:
                data = json.loads(data_text)
                price = 'offers' in data and data['offers']['price'] or ""
                price = format_pl_price(str(price))

                # Check if availability indicates out of stock
                if 'offers' in data and 'availability' in data['offers']:
                    if 'OutOfStock' in data['offers']['availability']:
                        stock_status = "Outstock"
                        price = "0.00"
            
            if not price:
                # Zadowolenie specific price selectors
                try:
                    price_element = response.xpath("//meta[@property='product:original_price:device_full_price']").get()
                    if price_element:
                        price = self.fix_price(price_element)
                except Exception as e:
                    pass
            
            if not stock_status:
                # Zadowolenie specific out-of-stock indicators
                if self.element_exists(response, "//script[@type='application/ld+json' and contains(text(),'priceCurrency') and contains(text(), 'OutOfStock')]"):
                    stock_status = "Outstock"
                    price = "0.00"
                elif self.element_exists(response, "//*[contains(text(), 'Produkt') and contains(text(), 'niedostępny')]"):
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