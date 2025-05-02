import scrapy

class ProductItem(scrapy.Item):
    bn_code = scrapy.Field()
    price_link = scrapy.Field()
    xpath_result = scrapy.Field()
    out_of_stock = scrapy.Field()
    market_player = scrapy.Field()