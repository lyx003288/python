# -*- coding: utf-8 -*-
import csv
import glob
import itertools
import json
import logging
import os
import urllib

import pymysql
import scrapy
from scrapy.exceptions import DropItem

from tutorial import myutil
from tutorial.items import QuoteItem

# 黑名单
BLACK_LIST = [
    'qq.com', '163.com', 'jd.com', 'taobao.com', 'excite.co.jp', 'yahoo.co.jp',
    'ezweb', 'i.softbank', 'hotmail', 'sina.com', 'docomo', 'icloud',
    'excite.jp', 'apple.com', 'msn.com', 'pttest', 'mailinator.com',
    '126.com', 'ptmind.com', 'me.com', 'nifty.com', 'gmail.com', 'sohu.com',
    'posteo.jp', 'adfasf.com', 'ocn.ne.jp', 'outlook', 'live.jp', 'usako.net',
    'dea-love.net', 'adfasf.com',  'yahoo.ne.jp', 'yahoo.com', 'ptthink',
    'eyou.com', 'dea-love.net'
]


@myutil.singleton
class DetectURL(object):
    def __init__(self):
        self.opener = urllib.request.build_opener()
        self.opener.addheaders = [('User-agent', 'Mozilla/49.0.2')]
        self.http_type = ['http://www.', 'http://', 'https://www.', 'https://']
        self.urls = {}
        self.failed_domains = {}
        self.success_domains = {}
        self.__load()

    def __del__(self):
        self.opener.close()

    def __save(self, domain, url):
        myutil.make_dir('record')

        is_have_head = True
        if not os.path.exists(myutil.DETECT_URL):
            is_have_head = False

        with open(myutil.DETECT_URL, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(
                f,
                dialect=myutil.MyDialect,
                fieldnames=['domain', 'url']
            )

            if not is_have_head:
                writer.writeheader()

            writer.writerow({'domain': domain, 'url': url})

    def __load(self):
        self.domain2url = {}
        if os.path.exists(myutil.DETECT_URL):
            with open(myutil.DETECT_URL, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self.domain2url[row['domain']] = row['url']

    def __is_cache_have(self, domain):
        return domain in self.domain2url.keys()

    def __get_url(self, domain):
        value = self.domain2url[domain]
        if value == 'none':
            return False, ''
        else:
            return True, value

    def detect(self, domain):
        if domain in self.success_domains.keys():
            return True, self.success_domains[domain]

        if domain in self.failed_domains.keys():
            return False, ''

        if self.__is_cache_have(domain):
            return self.__get_url(domain)

        for http in self.http_type:
            url = http + domain
            logging.info('detecting url %s', url)
            try:
                response = self.opener.open(url, timeout=15)
                if response.status == 200:
                    detect_url = response.url.strip().lower()
                    self.success_domains[domain] = detect_url
                    self.__save(domain, detect_url)
                    return True, detect_url
            except Exception as _e:
                logging.info('detect failed %s', url)
                continue

        self.failed_domains[domain] = True
        self.__save(domain, 'none')
        return False, ''


@myutil.singleton
class HandleMysqlData(object):
    def __init__(self):
        self.data_list = []

    def download_mysql_data(self):
        db_info = myutil.get_db_info()
        conn = pymysql.connect(
            host=db_info['host'],
            port=db_info['port'],
            user=db_info['user'],
            password=db_info['passwd'],
            db='ptmind_common',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        with conn.cursor() as cursor:
            sql = 'SELECT * FROM %s order by id asc' % myutil.TABLE_USER_DETAIL
            cursor.execute(sql)
            for row in cursor:
                email = row['email'].strip().rstrip('/')
                if len(email.split('@')) > 1:
                    self.data_list.append(row)

        conn.close()

        with open(myutil.SAVE_FILE, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(
                f,
                dialect=myutil.MyDialect,
                fieldnames=myutil.FIELDS_USER_DETAIL
            )
            writer.writeheader()
            for row in self.data_list:
                writer.writerow(row)

    def get_data_list(self):
        # return self.data_list
        return [{'email': 'yaxin.liu@gaiax.com'}]
        # return [
        #     {'email': 'yaixn.liu@ptmind.com'},
        #     {'email':'yuhtaohishi@gmail.com'},
        #     {'email':'yuhtaohishi@libevent.org'},
        #     {'email':'justastrayghost@gmail.com'},
        #     {'email':'urikinayeto@gmail.com'},
        #     {'email':'ueda@hoei-kikaku.jp'},
        #     {'email':'info@agentspider.jp'},
        #     {'email':'yoshi@good-smile21.jp'},
        #     {'email':'iwasawa@gcrest.com'}
        # ]


class QuotesSpider(scrapy.Spider):
    name = 'quotes'
    allowed_domains = []
    start_urls = []
    handle_httpstatus_list = [400, 403, 404, 429, 500, 503, 511, 524]

    all_urls = []
    # 计数器
    count = itertools.count(1)
    cur_count = 0

    ext_list = [
        '.txt', '.pdf', '.json', '.exe', '.doc', '.jpg',
        '.png', '.gif', '.tif',  '.bmp', '.rtf', '.xml',
        '.eml', '.dbx', '.pst', '.xls',  '.wps', '.mdb',
        '.wpd', '.zip', '.rar', '.7z', '.tgz', '.wav', '.wmv',
        '.avi', '.ram', '.rm', '.mpg', '.mov', '.asf', '.mid',
        '.space'
    ]

    def __init__(self):
        myutil.make_dir('record')
        HandleMysqlData().download_mysql_data()

    def start_requests(self):
        data_list = HandleMysqlData().get_data_list()
        line_number = 0
        for item in data_list:
            if self.__is_reach_limit():
                break

            line_number += 1
            logging.info('start_requests: line %s, email %s',
                         line_number, item['email'])
            e = item['email']
            d = myutil.get_domain(e)
            if len(d) == 0:
                continue
            if d in self.allowed_domains:
                continue

            if self.__check_domain(d) == False:
                continue

            is_have, sdir = self.__is_have_offline_data(d)
            if is_have:
                continue

            OK, url = DetectURL().detect(d)
            if not OK:
                continue
            if url in self.start_urls:
                continue

            if self.__check_url(url) == False:
                continue

            self.allowed_domains.append(d)
            self.start_urls.append(url)
            self.all_urls.append(url)
            it = {'sdir': sdir, 'domain': d}
            yield scrapy.Request(url=url, callback=lambda response, item=it: self.parse(response, item), errback=self.error_cb, dont_filter=True)

    def error_cb(self, response):
        pass

    def parse(self, response, it):
        logging.info('start scrapy: %s, sdir=%s', response.url, it['sdir'])

        try:
            if response.status in self.handle_httpstatus_list:
                return DropItem('response status error')

            item = QuoteItem()
            item['domain'] = it['domain']
            item['url'] = response.url.strip().rstrip(r'/')
            item['phrase'] = 'first_page'
            item['sdir'] = it['sdir']
            item['text'] = response.text.strip()
            yield item

        except AttributeError as _err:
            return DropItem('attr error')

        tag_a_list = response.selector.xpath('//a')
        for tag_a in tag_a_list:
            href = tag_a.xpath('@href').extract_first('').strip()
            text = tag_a.xpath('text()').extract_first('none').strip()
            if len(href) > 0:
                url = response.urljoin(href).rstrip('/')
                if self.__check_url(url) == False:
                    continue

                # 排除不是本域名的url
                if it['domain'] not in url:
                    continue

                if url not in self.all_urls:
                    self.all_urls.append(url)
                    it['phrase'] = text
                    yield scrapy.Request(url=url, callback=lambda response, item=it: self.parse_sub_url(response, it), dont_filter=True)

    def parse_sub_url(self, response, it):
        try:
            if response.status in self.handle_httpstatus_list:
                return DropItem('response status error')

            item = QuoteItem()
            item['domain'] = it['domain']
            item['url'] = response.url.strip().rstrip(r'/')
            item['phrase'] = it['phrase']
            item['sdir'] = it['sdir']
            item['text'] = response.text.strip()
            yield item

        except AttributeError as _err:
            return DropItem('attr error')

    def __is_have_offline_data(self, domain):
        sdir = myutil.get_sdir(domain)
        path_name = os.path.join('.', 'data', sdir, domain, "*.json")
        data_files = glob.glob(path_name)
        if len(data_files) > 0:
            return True, sdir

        return False, sdir

    def __is_reach_limit(self):
        self.__add_count()
        if self.cur_count > myutil.get_urls_limit():
            logging.info('run limit reached !')
            return True
        return False

    def __add_count(self):
        self.cur_count = next(self.count)

    def __check_domain(self, domain):
        if len(domain) == 0:
            return False

        item_list = domain.split(r'.')
        item_list = item_list[:-1]
        for b in item_list:
            if b in BLACK_LIST:
                return False

        for b in BLACK_LIST:
            if b in domain:
                return False

        return True

    def __check_url(self, url):
        if type(url) != type('str'):
            return False

        url = url.lower()
        _root, ext = os.path.splitext(url)
        if ext in self.ext_list:
            return False

        if url.startswith(r'mailto:'):
            return False

        if url.startswith(r'javascript:'):
            return False

        if url.startswith(r'tel:'):
            return False

        if url.startswith(r'comgooglemaps:'):
            return False

        if url.startswith(r'waze:'):
            return False

        if url.startswith(r'telto:'):
            return False

        return True
