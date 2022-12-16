# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import logging

import pymongo
import scrapy
from itemadapter import ItemAdapter
from scrapy.pipelines.images import ImagesPipeline

from FacebookContentImage.items import FacebookcontentimageItem
from FacebookContentImage.settings import MONGO_HOST, MONGO_PORT, MONGO_USERNAME, MONGO_PASSWORD, MONGO_SET_2


class FacebookcontentimagePipeline:
    def process_item(self, item, spider):
        return item


class FacebookMongoDB(scrapy.Item):
    def open_spider(self, spider):
        self.conn = pymongo.MongoClient(
            MONGO_HOST,
            MONGO_PORT
        )
        # self.db = self.conn[MONGO_DB]
        # self.myset = self.db[MONGO_SET]

    def process_item(self, item, spider):
        print("--------", item.keys())
        if item['col'] in ['kooler_kol_list', 'kooler_post_list']:
            db_name = 'kooler_'
            db = self.conn['kooler_buffer']

        elif item['col'] in ['brand_kol_list']:
            db_name = 'brand_'
            db = self.conn['brand_buffer']
        str_dict = {
            # "_id": item["object_id"],
            "type": item["type"],
            "channel": item["channel"],
            "url": item["url"],
            "publish_time": item["publish_time"],
            "platform_id": item["platform_id"],
            "crawl_time": item["crawl_time"],
            "store_time": item["store_time"],
            "crawl_time_log": item["crawl_time_log"],
            "store_time_log": item["store_time_log"],
            "data": item["data"]
        }

        db.authenticate(MONGO_USERNAME, MONGO_PASSWORD, source='admin', mechanism='SCRAM-SHA-256')

        myset = db[MONGO_SET_2]

        myset.insert_one(str_dict)

        return item


class ImageDownload(ImagesPipeline):
    headers = {
        # "Proxy-Authorization": xun_proxy()['auth'],
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7"
    }

    def get_media_requests(self, item, info):
        if isinstance(item, FacebookcontentimageItem):
            if item['col'] in ['kooler_kol_list', 'kooler_post_list']:
                bucket_name = 'kooler_buffer'

            elif item['col'] in ['brand_kol_list']:
                bucket_name = 'brand_buffer'

            else:
                raise ValueError("pipeline image dowload bucket_name")

            if item.get("head_img_info"):

                yield scrapy.Request(item["head_img"]["url"], headers=self.headers, meta={
                    "user_id": item["user_id"], "img_type": "head_img", 'bucket_name': bucket_name,
                    "platform_id": item["platform_id"], "kol_account_id": item["kol_main_id"],
                    "task_id": item["task_id"]
                })

            elif item['data'].get("image_info"):

                img_lst = item['data']["image_info"]

                for img_i in img_lst:
                    img_id = img_i["id"]
                    photo_url_2 = img_i["url"]

                    if "http" not in photo_url_2:
                        photo_url_2 = "https:" + photo_url_2

                    if photo_url_2:

                        print('img_url--ddd->', photo_url_2)
                        yield scrapy.Request(photo_url_2, headers=self.headers, meta={
                            "img_id": img_id, "content_id": item['data']["content_id"],
                            "img_type": "content_img", "bucket_name": bucket_name,
                            "platform_id": item["platform_id"], "kol_account_id": item["kol_main_id"],
                            "task_id": item["task_id"], "kol_main_id": item["kol_main_id"]
                        })
                    else:
                        with open("image_error.txt", 'a') as f:
                            f.write('content_image' + photo_url_2 + '\n')

    def file_path(self, request, response=None, info=None):
        if request.meta["img_type"] == "head_img":
            user_id = request.meta["user_id"]
            brand_name = request.meta["user_id"].replace('/', '_')
            url = request.url

            gcp_path = request.meta['bucket_name'] + '/facebook' + '/head_img' + '/%s' % str(user_id) + '/%s' % str(
                brand_name) + '.jpg'

            logging.info(gcp_path)

            return gcp_path

        elif request.meta["img_type"] == "content_img":
            content_id = request.meta["content_id"]
            img_id = request.meta["img_id"]

            gcp_path = request.meta['bucket_name'] + '/facebook/' + 'content/img' + '/%s' % str(
                content_id) + '/%s' % str(img_id) + '.jpg'

            logging.info(gcp_path)

            return gcp_path

    def item_completed(self, results, item, info):
        return item


