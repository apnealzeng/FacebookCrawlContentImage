import datetime
import re
from urllib import parse

import pymongo
import pytz
import scrapy
from lxml import etree

from FacebookContentImage.items import FacebookcontentimageItem
from FacebookContentImage.settings import MONGO_HOST, MONGO_USERNAME, MONGO_PASSWORD, MONGO_PORT, IMAGES_STORE


class FacebookcontentimageSpider(scrapy.Spider):
    name = 'facebookcontentimage'
    allowed_domains = ['facebook.com']
    # start_urls = ['http://facebook.com/']

    def start_requests(self):
        conn = f'mongodb://{parse.quote_plus(MONGO_USERNAME)}:{parse.quote_plus(MONGO_PASSWORD)}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin&authMechanism=SCRAM-SHA-256'
        client = pymongo.MongoClient(conn)

        data_lst = client["potential_buffer"]["facebook_image"].find({"url": {"$regex": "story_fbid="}})
        print(data_lst.count())
        for data in data_lst[0:1]:

            # content_url = "https://www.facebook.com/permalink.php?story_fbid=2787143381295641&id=100000000202403&substory_index=0"
            # content_url = "https://www.facebook.com/permalink.php?story_fbid=4572263482783613&id=100000000202403"

            # content_url = "https://www.facebook.com/permalink.php?story_fbid=2646752812001366&id=100000000202403"

            # content_url = "https://www.facebook.com/permalink.php?story_fbid=4727126483963978&id=100000000202403"
            content_url = data["url"]
            print(content_url)
            headers = {
                'authority': 'www.facebook.com',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'accept-language': 'zh-CN,zh;q=0.9',
                'cache-control': 'no-cache',
                'cookie': 'fr=04fG66KDD9PucCG48..BjmszE.nC.AAA.0.0.BjmszE.AWW5K2HgANg; sb=xMyaYxwbJxbKZMVK3SejXLbl; datr=xMyaY9jXBO3FXFImEHdQtZXx; wd=1009x969; fr=04fG66KDD9PucCG48..BjmszE.nC.AAA.0.0.Bjms1s.AWX8qZKQ-Jo',
                'pragma': 'no-cache',
                'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'none',
                'sec-fetch-user': '?1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
            }

            yield scrapy.Request(content_url, headers=headers, callback=self.parse_url, meta={
                "post_data": data
            })

    def parse_url(self, response):
        item = FacebookcontentimageItem()
        # image_url = response.xpath("//meta[@property='og:image']/@content").extract()
        #
        # print(image_url)

        # 寻找贴文是否有图片
        content_text = response.xpath("//div[@class='_4-u2 mbm _4mrt _5v3q _7cqq _4-u8']").extract()

        # 贴文时间戳
        publish_time = response.xpath("//abbr/@data-utime")

        html_obj = etree.HTML(content_text[0])
        image_url_lst = html_obj.xpath("//a[@rel='theater']/@data-ploi")
        print(image_url_lst, len(image_url_lst))

        if image_url_lst:

            beijing = pytz.timezone('Asia/Shanghai')
            date_1 = datetime.datetime.utcnow().replace(microsecond=0) + datetime.timedelta(hours=8)
            image_info_lst = list()

            if "potential" in response.meta["post_data"]["type"]:
                bucket_name = "potential_buffer"
            elif "kooler" in response.meta["post_data"]["type"]:
                bucket_name = "kooler_buffer"
            elif "brand" in response.meta["post_data"]["type"]:
                bucket_name = "brand_buffer"
            else:
                raise ValueError("spider parse_url bucket_name")

            for image in image_url_lst:
                content_img_id = re.findall(r"_(.*?)_", image)[0]
                content_img_url = image
                image_path = IMAGES_STORE + bucket_name + '/facebook/' + 'content/img' + '/%s' % str(response.meta["post_data"]["data"]["content_id"]) + '/%s' % str(content_img_id) + '.jpg'
                image_info_lst.append(
                    {
                        "id": content_img_id,
                        "url": content_img_url,
                        "path": image_path
                    }
                )
            try:
                time1 = datetime.datetime.utcfromtimestamp(int(publish_time)).replace(microsecond=0) + datetime.timedelta(hours=8)
                p_time = time1.astimezone(beijing).isoformat()
            except Exception as e:
                p_time = ""

            crawl_times = date_1.astimezone(beijing).isoformat()
            item["type"] = response.meta["post_data"]["type"]
            item["channel"] = "facebook"
            item["url"] = response.url
            item["publish_time"] = p_time
            item["platform_id"] = response.meta["post_data"]["platform_id"]
            item["crawl_time"] = crawl_times
            item["store_time"] = crawl_times
            item["crawl_time_log"] = [
                crawl_times
            ]
            item["store_time_log"] = [
                crawl_times
            ]
            brand_mention_str_lst = re.findall(r'@(\w+)', response.meta["post_data"]["data"]["content"])

            if brand_mention_str_lst:
                brand_mention = [
                    {
                        "type": "string", "value": brand_mention_str_lst
                    },
                    {
                        "type": "link", "value": []
                    },
                    {
                        "type": "img", "value": []
                    }
                ]
            else:
                brand_mention = []

            item["data"] = {
                "content_id": response.meta["post_data"]["data"]["content_id"],
                "homepage_url": "",
                "hash_tag": re.findall(r'#(\w+)', response.meta["post_data"]["data"]["content"]),
                "lang": "",
                "title": "",
                "sub_title": "",
                "content": response.meta["post_data"]["data"]["content"],
                "other_content": response.meta["post_data"]["data"]["other_content"],
                "brand_mention": brand_mention,
                "content_level": "0",
                "share_content": response.meta["post_data"]["data"]["share_content"],
                "image_info": image_info_lst,
                "video_info": response.meta["post_data"]["data"]["video_info"],
                "sn_interact_num": response.meta["post_data"]["data"]["sn_interact_num"]
            }

            item['job_uid'] = response.meta['job_uid']
            item['col'] = response.meta['col']
            item['kooler_ref_id'] = response.meta['kooler_ref_id']

            yield item

