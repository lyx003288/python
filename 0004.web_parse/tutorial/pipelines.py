# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import logging
import os
import json
import hashlib

from scrapy.exceptions import DropItem
from tutorial import myutil


class BodyPipeline(object):
    def process_item(self, item, spider):
        if spider.name != 'quotes':
            return item

        if len(item) == 0:
            return DropItem('no item')

        save_dir = os.path.join('.', 'data', item['sdir'], item['domain'])
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        url_hash_str = hashlib.md5(item['url'].encode('utf-8')).hexdigest()
        file = os.path.join(save_dir, url_hash_str+'.json')
        if os.path.exists(file) and item['phrase'] != 'first_page':
            return DropItem('item aready existed')

        with open(file, 'w', newline='', encoding='utf-8') as f:
            json.dump(dict(item), f, ensure_ascii=False)

        return item


class PartnerPipeline(object):
    def process_item(self, item, spider):
        if spider.name != 'partner':
            return item

        if len(item) == 0:
            return DropItem('no item')

        save_dir = os.path.join('.', 'partner', item['sdir'], item['domain'])
        url_hash_str = hashlib.md5(item['url'].encode('utf-8')).hexdigest()
        file = os.path.join(save_dir, url_hash_str+'.json')
        if os.path.exists(file) and item['phrase'] != 'first_page':
            return DropItem('item aready existed')

        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        with open(file, 'w', newline='', encoding='utf-8') as f:
            json.dump(dict(item), f, ensure_ascii=False)

        return item
