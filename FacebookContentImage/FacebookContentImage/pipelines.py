# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import datetime
import logging
import re
from urllib import parse

import pymongo
import pytz
import scrapy
from itemadapter import ItemAdapter
from scrapy.pipelines.images import ImagesPipeline
from scrapy.pipelines.files import FilesPipeline, logger, FileException
from scrapy.utils.request import referer_str
from FacebookContentImage.items import FacebookcontentimageItem
from FacebookContentImage.settings import MONGO_HOST, MONGO_PORT, MONGO_USERNAME, MONGO_PASSWORD, MONGO_SET_2, \
    MONGO_SET_OLD, FILES_STORE


class FacebookcontentimagePipeline:
    def process_item(self, item, spider):
        print(item)
        return item


class FacebookMongoDB(object):
    def open_spider(self, spider):
        conn_str = f'mongodb://{parse.quote_plus(MONGO_USERNAME)}:{parse.quote_plus(MONGO_PASSWORD)}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin&authMechanism=SCRAM-SHA-256'
        self.conn = pymongo.MongoClient(conn_str)

    def process_item(self, item, spider):
        print("--------", item.keys())
        if item['col'] in ['kooler_kol_list', 'kooler_post_list']:
            db_name = 'kooler_'
            db = self.conn['kooler_buffer']

        elif item['col'] in ['brand_kol_list']:
            db_name = 'brand_'
            db = self.conn['brand_buffer']
        elif item['col'] in ['potential_kol_list']:
            db_name = 'brand_'
            db = self.conn['potential_buffer']
        str_dict = {
            "type": db_name + item["type"],
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

        myset = db[MONGO_SET_2]

        myset.insert_one(str_dict)

        myset_old = db[MONGO_SET_OLD]

        myset_old.update_many(
            {
                "data.content_id": item["data"]["content_id"]
            },
            {
                "$set": {
                    "ref_date_time": datetime.datetime.now()
                }
            }
        )

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

            elif item['col'] in ['potential_kol_list']:
                bucket_name = 'potential_buffer'

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
                            "platform_id": item["platform_id"]
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


class VideoDownload(FilesPipeline):
    headers = {
        # "Proxy-Authorization": xun_proxy()['auth'],
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Range": "bytes=0-52428800"
    }

    def get_media_requests(self, item, info):
        if isinstance(item, FacebookcontentimageItem):
            if item['col'] in ['kooler_kol_list', 'kooler_post_list']:
                bucket_name = 'kooler_buffer'

            elif item['col'] in ['brand_kol_list']:
                bucket_name = 'brand_buffer'

            elif item['col'] in ['potential_kol_list']:
                bucket_name = 'potential_buffer'

            if item['data']["video_info"]:

                video_lst = item['data']["video_info"]

                for video_i in video_lst:

                    video_id = video_i["id"]

                    photo_url_2 = video_i["url"]
                    if "http" not in photo_url_2:
                        photo_url_2 = "https:" + photo_url_2
                    if photo_url_2:

                        print('video_url--ddd->', photo_url_2)
                        logging.info('video_url--ddd-> ' + photo_url_2)
                        r_p = re.findall(r'https://(.*?)/v', photo_url_2)[0]
                        photo_url_2 = photo_url_2.replace(r_p, 'video.xx.fbcdn.net')

                        yield scrapy.Request(photo_url_2, headers=self.headers, meta={
                            "video_id": video_id, "content_id": item['data']["content_id"],
                            "img_type": "content_video", "bucket_name": bucket_name,
                            "platform_id": item["platform_id"]
                        })

                    else:
                        with open("video_error.txt", 'a') as f:
                            f.write('content_image' + photo_url_2 + '\n')

    def media_downloaded(self, response, request, info, *, item=None):
        referer = referer_str(request)

        if response.status not in [200, 206]:
            logger.warning(
                'File (code: %(status)s): Error downloading file from '
                '%(request)s referred in <%(referer)s>',
                {'status': response.status,
                 'request': request, 'referer': referer},
                extra={'spider': info.spider}
            )
            raise FileException('download-error')

        if not response.body:
            logger.warning(
                'File (empty-content): Empty file from %(request)s referred '
                'in <%(referer)s>: no-content',
                {'request': request, 'referer': referer},
                extra={'spider': info.spider}
            )
            raise FileException('empty-content')

        status = 'cached' if 'cached' in response.flags else 'downloaded'
        logger.debug(
            'File (%(status)s): Downloaded file from %(request)s referred in '
            '<%(referer)s>',
            {'status': status, 'request': request, 'referer': referer},
            extra={'spider': info.spider}
        )
        self.inc_stats(info.spider, status)

        try:
            path = self.file_path(request, response=response, info=info, item=item)
            checksum = self.file_downloaded(response, request, info, item=item)
        except FileException as exc:
            logger.warning(
                'File (error): Error processing file from %(request)s '
                'referred in <%(referer)s>: %(errormsg)s',
                {'request': request, 'referer': referer, 'errormsg': str(exc)},
                extra={'spider': info.spider}, exc_info=True
            )
            raise
        except Exception as exc:
            logger.error(
                'File (unknown-error): Error processing file from %(request)s '
                'referred in <%(referer)s>',
                {'request': request, 'referer': referer},
                exc_info=True, extra={'spider': info.spider}
            )
            raise FileException(str(exc))

        return {'url': request.url, 'path': path, 'checksum': checksum, 'status': status}

    def file_path(self, request, response=None, info=None):

        if request.meta["img_type"] == "content_video":
            content_id = request.meta["content_id"]
            video_id = request.meta["video_id"]

            gcp_path = request.meta['bucket_name'] + '/facebook/' + 'content/video' + '/%s' % str(
                content_id) + '/%s' % str(video_id) + '.mp4'

            logging.info(gcp_path)

            return gcp_path

    def item_completed(self, results, item, info):
        return item

