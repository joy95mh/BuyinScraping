# BuyinScraping/src/utils/__init__.py

from src.utils.proxy_manager import ProxyManagerFreeProxyListDotNet

def configure_proxy_manager(settings):
    """
    Configure the Scrapy settings to use the FreeProxyListDotNet proxy manager.
    
    Args:
        settings: The Scrapy settings dictionary to modify
        
    Returns:
        The proxy manager instance that was created
    """
    # Create a new instance of the proxy manager
    proxy_manager = ProxyManagerFreeProxyListDotNet()
    
    # Configure settings for proxy middleware if needed
    settings.set('DOWNLOADER_MIDDLEWARES', {
        'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
    })
    
    # Add proxy manager to the crawler
    settings.set('PROXY_MANAGER', proxy_manager)
    
    # Return the proxy manager instance for direct use
    return proxy_manager