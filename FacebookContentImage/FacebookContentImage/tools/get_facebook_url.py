#!/usr/bin/env python
# encoding: utf-8
'''
@File    :   get_facebook_url
@Time    :   2022/12/19 11:01
@Author  :   Neal
@Version :   1.0
@Contact :   Neal
@License :   (C)Copyright 2021
@Desc    :   
'''
import json
import random
import re
import time
import redis
import pymongo
import configparser
import requests


class MongodbInit(object):
    def __init__(self, ip, port, dbname, collection, username=None, password=None) -> None:
        super().__init__()
        self.conn = pymongo.MongoClient(
            ip,
            port
        )

        self.db = self.conn[dbname]

        if username:
            self.db.authenticate(username, password,
                                 source='admin', mechanism='SCRAM-SHA-256')

        self.myset = self.db[collection]
        # self.db1 = self
        # print(list(self.myset.find({'platform_id': '349148306581039'})))

    def get_conn(self) -> object:

        return self.conn

    def get_db(self):

        return self.db

    def get_myset(self):

        return self.myset


class MongodbProcess(object):

    def process_select(self, myset, command1, command2=None):
        lst = myset.find(command1, command2)

        return list(lst)

    def process_update(self, myset, command1, command2, upsert=False):
        myset.update_one(
            command1, command2, upsert
        )


class RedisInit(object):

    def __init__(self, db_type):
        config = configparser.ConfigParser()
        config.read('C:\\Users\\asiapac\\Desktop\\AsiapacSpider\\facebook_setting\\database_config.ini')
        # config.read('C:\\Users\\apuser\\Desktop\\Asiapc_spider\\kooler_spider\\KoolerPostList\\KoolerPostList\\tools\\database_config.ini')
        self.redis_host = config.get(db_type, 'host')
        self.redis_port = config.getint(db_type, 'port')
        self.redis_db = config.getint(db_type, 'db')
        # print(type(self.redis_host), self.redis_host)
        # print(type(self.redis_port), self.redis_port)

    # 使用REDIS中的cookie
    def get_redis_data(self):
        # 连接REDIS
        r_conn = redis.Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db, password='neal188')
        # 获取所有属性名
        data_lst = r_conn.hkeys('facebook')

        if not data_lst:
            time.sleep(30)
            data_lst = r_conn.hkeys('facebook')

        # 随机获取一个cookie_data
        r_field = random.choice(data_lst).decode()
        cookie_data = r_conn.hget('facebook', r_field)

        return r_field, json.loads(cookie_data)

    # 更新REDIS中的COOKIE
    def update_redis_data(self, r_field, cookie_data):
        # 连接REDIS
        r_conn = redis.Redis(host=self.redis_host, port=self.redis_port, db=1, password='neal188')

        r_conn.hset("facebook_error", r_field, str(cookie_data))

    def del_redis_data(self, r_field):
        r_conn = redis.Redis(host=self.redis_host, port=self.redis_port, db=1, password='neal188')

        r_conn.hdel('facebook', r_field)


class GetFacebookUserUrl(object):

    def __init__(self):
        pass
        # self.myset = MongodbInit(
        #     '34.150.113.172', 27017, 'test_potential_buffer', 'new_channel', "devkooler01",
        #     "Wyx!$rmf!g@k39r").get_myset()
        # self.myset_kol_channel = MongodbInit(
        #     '34.150.113.172', 27017, 'potential_buffer', 'kol_channel', "devkooler01",
        #     "Wyx!$rmf!g@k39r").get_myset()

    def get_url(self):
        platform_id_lst = [

            "100044290576608",
        ]
        url_lst = list()
        for platform_id in platform_id_lst[0:1]:
            # print()
            data_lst = MongodbProcess().process_select(self.myset_kol_channel, {'kol_channel.platform_id': platform_id})
            channel_lst = data_lst[0]['kol_channel']
            for channel in channel_lst:
                if channel['channel'] == 'facebook':
                    url_lst.append(channel['channel_url'])

        return url_lst

    def deal_url(self, url, cookie):
        headers = {
            'authority': 'www.facebook.com',
            'cache-control': 'max-age=0',
            'sec-ch-ua': '"Google Chrome";v="93", " Not;A Brand";v="99", "Chromium";v="93"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-user': '?1',
            'sec-fetch-dest': 'document',
            'accept-language': 'zh-CN,zh;q=0.9',
            # 'cookie': 'sb=6s81YRGhlqiKX3J4Icl_T6Ub; m_pixel_ratio=1; x-referer=eyJyIjoiL2hvbmdrb25nbWFrZXVwZm9yZXZlci8iLCJoIjoiL2hvbmdrb25nbWFrZXVwZm9yZXZlci8iLCJzIjoibSJ9; datr=eDNVYTVahb2DET2jcxL7quF_; wd=1920x969; locale=en_US; c_user=100073933441459; xs=17%3AwwIY7dtmRRwIlA%3A2%3A1634006243%3A-1%3A-1; fr=0fcVNigfi8kD7TIGw.AWWu4z5vhfEJXZRO5AkgiSkWMus.BhVTN6.bE.AAA.0.0.BhZPTa.AWVhatWx_zQ; spin=r.1004533939_b.trunk_t.1634006244_s.1_v.2_; c_user=100073360268833; fr=0DtGQECjCCv2W7Nii.AWVO26pg2CYeVIWcfgcCSAxXGWk.BhY5po.qc.AAA.0.0.BhY5po.AWXxizPAshk; xs=33%3AX7ydakpRhQFEtA%3A2%3A1632974411%3A-1%3A-1%3A%3AAcWyR2NdYoUD0lVl-vPR7O8e17TG6BGqLyCwC17o7uo'
        }

        # r_field, cookie_data = RedisInit('redis_hongkang').get_redis_data()

        try:
            res = requests.get(url, headers=headers, cookies=cookie)
        except Exception as e:
            # , proxies={'http': 'http://37.48.118.4:13081'}, timeout=0.5
            # res = requests.get(url, headers=headers, proxies={
            #                    'http': 'http://37.48.118.4:13081'}, timeout=0.5)
            res = ''
        # with open('page.txt', 'w', encoding='utf-8') as f:
        #     f.write(res.text)
        if res:
            res.encoding = "utf-8"
            try:
                page_id = re.findall(r'"pageID":"(.*?)",', res.text)[0]
            except Exception as e:
                page_id = ''
            print("page_id--->", page_id)
            if page_id:

                return {'name': 'page_id', 'value': page_id}

            try:
                user_id = re.findall(r'"userID":"(.*?)",', res.text)[0]
            except Exception as e:
                user_id = ''
            if not user_id:
                try:
                    user_id = re.findall(r'"userID":(.*?),', res.text)[0]
                except Exception as e:
                    user_id = ''
            if user_id:
                try:
                    user_id = str(int(user_id))
                except Exception as e:
                    user_id = str(int(re.findall(r'(.*?)"}', user_id)[0]))
            print("user_id--->", user_id)

            if user_id:

                return {'name': 'user_id', 'value': user_id}

                # print('url---->', url)
            if page_id == user_id:
                return {'name': 'error', 'value': 1}
        else:
            return {'name': 'error', 'value': 1}


