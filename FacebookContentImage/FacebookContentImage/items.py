# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
import pymongo
import scrapy


class FacebookcontentimageItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    type = scrapy.Field()
    channel = scrapy.Field()
    url = scrapy.Field()
    publish_time = scrapy.Field()
    platform_id = scrapy.Field()
    crawl_time = scrapy.Field()
    store_time = scrapy.Field()
    crawl_time_log = scrapy.Field()
    store_time_log = scrapy.Field()
    data = scrapy.Field()
    col = scrapy.Field()



