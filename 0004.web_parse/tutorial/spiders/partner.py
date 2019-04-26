# -*- coding: utf-8 -*-
import csv
import logging
import os
import glob

import scrapy
from scrapy.exceptions import DropItem

from tutorial import myutil
from tutorial.items import QuoteItem


class PartnerSpider(scrapy.Spider):
    name = 'partner'
    allowed_domains = []
    start_urls = []
    custom_settings = {
        'ITEM_PIPELINES': { 
            'tutorial.pipelines.PartnerPipeline': 300,
        }
    }
    handle_httpstatus_list = [400, 403, 404, 429, 500, 503, 511, 524]

    ext_list = [
        '.txt', '.pdf', '.json', '.exe', '.doc', '.jpg',
        '.png', '.gif', '.tif',  '.bmp', '.rtf', '.xml',
        '.eml', '.dbx', '.pst', '.xls',  '.wps', '.mdb',
        '.wpd', '.zip', '.rar', '.7z', '.tgz', '.wav', '.wmv',
        '.avi', '.ram', '.rm', '.mpg', '.mov', '.asf', '.mid',
        '.space'
    ]

    partner_data = []

    def __init__(self):
        myutil.make_dir('partner')

    def start_requests(self):
        self.__load_partner()

        for partner in self.partner_data:
            url = partner['URL'].lower().rstrip(r'/')
            logging.info('start_requests: %s', url)

            is_ok, domain = self.__extract_domain(url)
            if not is_ok:
                continue

            is_have, sdir = self.__is_have_offline_data(domain)
            if is_have:
                continue

            if not self.__check_url(url):
                continue

            it = {'sdir': sdir,  'domain': domain}
            yield scrapy.Request(url=url, callback=lambda response, item=it: self.parse(response, item), errback=self.error_cb)

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

                it['phrase'] = text
                yield scrapy.Request(url=url, callback=lambda response, item=it: self.parse_sub_url(response, it))

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

    def __load_partner(self):
        partner_file = os.path.join('record', 'partner.csv')
        with open(partner_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.partner_data.append(row)

    def __extract_domain(self, url):
        if url.startswith(r'http://www.'):
            return True, url.replace(r'http://www.', '')

        if url.startswith(r'https://www.'):
            return True, url.replace(r'https://www.', '')

        if url.startswith(r'http://'):
            return True, url.replace(r'http://', '')

        if url.startswith(r'https://'):
            return True, url.replace(r'https://', '')

        return False, None

    def __is_have_offline_data(self, domain):
        sdir = myutil.get_sdir(domain)
        path_name = os.path.join('.', 'partner', sdir, domain, "*.json")
        data_files = glob.glob(path_name)
        if len(data_files) > 0:
            return True, sdir

        return False, sdir

    def __check_url(self, url):
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
