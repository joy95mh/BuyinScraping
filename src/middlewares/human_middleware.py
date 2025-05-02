import random
from scrapy.downloadermiddlewares.retry import RetryMiddleware

class ProxyMiddleware:
    def __init__(self):
        with open("configs/proxies.txt", "r") as f:
            self.proxies = [line.strip() for line in f if line.strip()]

    def process_request(self, request, spider):
        if spider.config.get("proxy_required"):
            request.meta["proxy"] = random.choice(self.proxies)
        return None

class CustomRetryMiddleware(RetryMiddleware):
    def process_response(self, request, response, spider):
        if response.status in [403, 429, 503]:
            return self._retry(request, "Bad response", spider) or response
        return response

class HumanBehaviorMiddleware:
    def process_request(self, request, spider):
        if random.random() < 0.1:
            request.headers["X-Requested-With"] = "XMLHttpRequest"
        request.headers["Referer"] = "https://www.google.com/"
        return None