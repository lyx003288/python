#!/usr/bin/env python3 -u
# -*- coding: utf-8 -*-

import csv
import getopt
import glob
import json
import logging
import logging.handlers
import os
import re
import sys
import time
from collections import Counter

import bs4
import nltk
import pymysql
from scrapy.cmdline import execute

G_CUR_FILE_PATH = os.path.split(os.path.realpath(__file__))[0]
os.chdir(G_CUR_FILE_PATH)   # 初始化工作路径

import myutil


# 查找关键词
KW_ARR = [
    '集客', '集客支援', 'デジタルマーケティング', 'デジタルマーケティング支援', 'SEO',
    'SEM', 'Web', 'Webデザイン', 'ウェブ', 'ウェブデザイン', 'EC', 'イーコマース',
    'eコマース', '運営', 'メディア', '戦略', '企画', 'プランニング', '広告', '代行',
    'UI', 'UX', 'データ', 'アクセス解析', 'サイト解析', 'ウェブ改善', 'ウェブ解析',
    '分析', '最適化', 'グロースハッカー', 'コンサルティング', 'ソフトウェア', 'IT',
    'マーケティング', 'ECサイト', '検索エンジン', 'プロモーション', 'リスティング',
    'ランディングページ', 'コンテンツ', 'アフィリエイト', 'インターネット', '代理店',
    '営業', 'データ', 'コミュニケーション', 'クリエーティブ', 'デジタル', 'PR',
    'ユーザーエクスペリエンス', '戦略立案', 'CRM', 'アナリティクス', 'クリエイター',
    'マーケター', 'デザイナー', 'ECサイト', 'インバウンドビジネス', 'インバウンドマーケティング'
]
# 对查找的关键词进行预处理：英文全变成小写的
KW_ARR = [kw.strip().lower() for kw in KW_ARR]

# 一些特殊的关键词 (key_word, pattern)
SPCE_KW_ARR = [
    (r'A/B test'.strip().lower(), r'\bA/B\stest\b'),
    (r'A/B テスト'.strip().lower(), r'\bA/B\sテスト'),
]
SPCE_KEY_ARR = [key for key, _ in SPCE_KW_ARR]

ALL_KW_ARR = KW_ARR + SPCE_KEY_ARR

EXT_PARTNER_KW_ARR = [
    'extension_partner', 'homepage', 'total_kw_hits', 'distinct_kw_hits'
]

SHOW_PARTNER_FIELDS = EXT_PARTNER_KW_ARR + ALL_KW_ARR


def my_excepthook(exc_type, exc_value, traceback):
    logging.error(
        "Uncaught Exception",
        exc_info=(exc_type, exc_value, traceback)
    )


def make_dir(name):
    if not os.path.exists(name):
        os.mkdir(name)


def init_log_config():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(filename)s %(funcName)s %(lineno)d] %(levelname)s: %(message)s',
        datefmt='[%m/%d %H:%M:%S]'
    )

    #################################################################################################
    formatter = logging.Formatter(
        '%(asctime)s [%(filename)s %(funcName)s %(lineno)d] %(levelname)s: %(message)s')

    debug_rotating = logging.handlers.RotatingFileHandler(
        "log/debug.log", maxBytes=1024*1024*20, backupCount=3, delay=True)
    debug_rotating.set_name('debug')
    debug_rotating.setLevel(logging.DEBUG)
    debug_rotating.setFormatter(formatter)
    logging.getLogger().addHandler(debug_rotating)

    info_rotating = logging.handlers.RotatingFileHandler(
        "log/info.log", maxBytes=1024*1024*10, backupCount=3, delay=True)
    debug_rotating.set_name('info')
    info_rotating.setLevel(logging.INFO)
    info_rotating.setFormatter(formatter)
    logging.getLogger().addHandler(info_rotating)

    error_rotating = logging.handlers.RotatingFileHandler(
        "log/error.log", maxBytes=1024*1024*10, backupCount=3, delay=True)
    debug_rotating.set_name('error')
    error_rotating.setLevel(logging.WARNING)
    error_rotating.setFormatter(formatter)
    logging.getLogger().addHandler(error_rotating)

    logging.getLogger("urllib3").setLevel(logging.WARNING)

    make_dir("log")


class ArgsException(Exception):
    pass


class MyException(Exception):
    pass


def spider_data():
    try:
        execute(['scrapy', 'crawl', 'partner'])
    except Exception as e:
        raise e


@myutil.singleton
class HandleStore(object):

    def __init__(self):
        self.parnter_fields = SHOW_PARTNER_FIELDS
        self.partners = []
        make_dir('record')

    def load_partner(self):
        partner_file = os.path.join('record', 'partner.csv')
        with open(partner_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.partners.append(row)

    def get_partners(self):
        return self.partners

    def handle_file_name(self, file_name):
        record_file = os.path.join('.', 'record', file_name)
        if os.path.exists(record_file):
            file_suffix = str(time.time())
            os.rename(record_file, record_file+r'.'+file_suffix)

            if os.path.exists(record_file):
                raise MyException('%s rename error' % file_name)

        return record_file

    def write_detail_head(self, file_name, mode='w'):
        with open(file_name, mode, newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(
                f,
                dialect=myutil.MyDialect,
                fieldnames=self.parnter_fields
            )
            writer.writeheader()

    def write_detail_row(self, file_name, row, mode='a'):
        with open(file_name, mode, newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(
                f,
                dialect=myutil.MyDialect,
                fieldnames=self.parnter_fields
            )
            writer.writerow(row)


@myutil.singleton
class ParseSpiderData(object):
    nltk_pattern = r'''(?x)     # set flag to allow verbose regexps
        (?:[A-Z]\.)+            # abbreviations, e.g. U.S.A.
        |\d+(?:\.\d+)?%?        # numbers, incl. currency and percentages
        |\w+(?:[-']\w+)*        # words w/ optional internal hyphens/apostrophe
        |\.\.\.                 # ellipsis
        |(?:[.,;"'?():-_`])     # special characters with meanings
    '''
    prog_list = []

    def __init__(self):
        for key_word, pattern in SPCE_KW_ARR:
            prog = re.compile(pattern, flags=re.I)
            self.prog_list.append((key_word, prog))

        self.parse_spider_dict = {}
        self.__load_parse()
        self.parse()

    def parse(self):
        serial_line = 0
        domain_dirs_path = os.path.join('.', 'partner', '*', '*')
        domain_dirs = glob.glob(domain_dirs_path)
        for data_path in domain_dirs:
            basename = os.path.basename(data_path)
            if self.__is_cache_parse(basename):
                logging.info('ParseSpiderData cache: %s', data_path)
                continue

            serial_line += 1
            logging.info(
                'ParseSpiderData parse %d: %s', serial_line, data_path
            )

            email_suffix = basename
            homepage = ''
            sum_kw_counter = Counter({})

            files_path = os.path.join(data_path, '**', '*.json')
            json_files = glob.glob(files_path, recursive=True)
            for json_file in json_files:
                with open(json_file, 'r', encoding='utf-8') as f:
                    try:
                        data_json = json.loads(f.read())

                        json_email_suffix = data_json['domain']

                        # 从首页中获取域名、url信息
                        if data_json['phrase'] == 'first_page':
                            email_suffix = json_email_suffix
                            homepage = data_json['url']

                        # 查看缓存
                        if self.__is_cache_parse(json_email_suffix):
                            continue

                        # 排除不是本域名的url
                        if json_email_suffix not in data_json['url']:
                            continue

                        _, sum_kw_counter = self.__do_parse(
                            data_json, sum_kw_counter)

                    except Exception as _e:
                        continue

            total_kw_hits = 0
            distinct_kw_hits = 0
            sum_kw_dict = dict(sum_kw_counter)
            for key in sum_kw_dict:
                count = sum_kw_dict[key]
                total_kw_hits += count
                distinct_kw_hits += 1

            sum_kw_dict['total_kw_hits'] = total_kw_hits
            sum_kw_dict['distinct_kw_hits'] = distinct_kw_hits

            if len(email_suffix) > 0:
                sum_kw_dict['email_suffix'] = email_suffix
                sum_kw_dict['homepage'] = homepage
                sum_kw_dict = self.__fill_default_value(sum_kw_dict)
                self.parse_spider_dict[email_suffix] = sum_kw_dict
                self.__save_parse(sum_kw_dict)

    def __save_parse(self, sum_kw_dict):
        save_file = os.path.join('record', 'parse_partner_spider.json')
        with open(save_file, 'a', newline='', encoding='utf-8') as f:
            json.dump(sum_kw_dict, f, ensure_ascii=False)
            f.write(os.linesep)

    def __load_parse(self):
        save_file = os.path.join('record', 'parse_partner_spider.json')
        if not os.path.exists(save_file):
            with open(save_file, 'w', encoding='utf-8') as f:
                pass

        with open(save_file, 'r', encoding='utf-8') as f:
            for line in f.readlines():
                sum_kw_dict = json.loads(line)
                email_suffix = sum_kw_dict['email_suffix']
                self.parse_spider_dict[email_suffix] = sum_kw_dict

    def __is_cache_parse(self, email_suffix):
        if email_suffix in self.parse_spider_dict.keys():
            return True

        return False

    def __do_parse(self, data_json, counter_summary):
        counter_detail = Counter({})
        spec_counter_detail = Counter({})
        text = data_json['text']
        soup = bs4.BeautifulSoup(text, 'lxml')
        tokens = [text.strip().lower() for text in soup.stripped_strings]
        for item in tokens:
            if self.__filter_text(item) == True:
                continue

            # 一些特殊字符的命中检查
            spec_hits = self.__get_hits(item)
            if len(spec_hits) > 0:
                spec_counter_detail = spec_counter_detail + Counter(spec_hits)

            words = nltk.regexp_tokenize(item, self.nltk_pattern)
            for word in words:
                if len(word) < 2:
                    continue

                hits = self.__check_hit(word)
                if len(hits) > 0:
                    counter_hits = Counter(hits)
                    counter_detail = counter_detail + counter_hits
                    counter_summary = counter_summary + counter_hits

        return counter_detail + spec_counter_detail, counter_summary + spec_counter_detail

    def __get_hits(self, item):
        hit_dict = {}
        for (key_word, prog) in self.prog_list:
            hits = prog.findall(item)
            if len(hits) > 0:
                hit_dict[key_word] = len(hits)

        return hit_dict

    def __filter_text(self, text):
        if len(text) < 2:
            return True

        # 去掉注释
        if text.startswith(r'<!--') and text.endswith(r'-->'):
            return True
        # 去掉注释
        if text.startswith(r'/*') and text.endswith(r'*/'):
            return True

        # 去掉 html 开头的text
        if text.startswith((
            r'html ', r'function', r'(function', r'$(function'
        )):
            return True

        return False

    def __check_hit(self, word):
        hit_dict = {}

        # 数字不进行匹配
        if word.isnumeric():
            return hit_dict

        # 英文字母必须完全匹配
        if word.encode('utf-8').isalpha():
            if word in KW_ARR:
                hit_dict[word] = 1
                return hit_dict
            else:
                return hit_dict

        # 模糊匹配
        for key_word in KW_ARR:
            count = word.count(key_word)
            if count > 0:
                hit_dict[key_word] = count

        return hit_dict

    def __fill_default_value(self, info_dict):
        for key_word in ALL_KW_ARR:
            if key_word not in info_dict.keys():
                info_dict[key_word] = 0
        return info_dict

    def get_info(self, domain):
        if domain in self.parse_spider_dict.keys():
            return self.parse_spider_dict[domain]

        return None


def parse_detail():
    partner_file = HandleStore().handle_file_name('partner_kw.csv')
    HandleStore().write_detail_head(partner_file)

    serial_line = 0
    HandleStore().load_partner()
    partners = HandleStore().get_partners()
    for partner in partners:
        extension_partner = partner[r'签约的partner'].lower().strip()
        url = partner['URL'].lower().rstrip(r'/')

        p = {
            'extension_partner': extension_partner,
            'homepage': url
        }

        serial_line += 1
        logging.info(
            'parse_data: serial num %d, url %s', serial_line, url
        )

        is_ok, domain = extract_domain(url)
        if not is_ok:
            HandleStore().write_detail_row(partner_file, p)
            continue

        kw_info = ParseSpiderData().get_info(domain)
        if kw_info is None:
            HandleStore().write_detail_row(partner_file, p)
            continue

        # 不需要显示此信息
        if 'email_suffix' in kw_info.keys():
            kw_info.pop('email_suffix')
        
        # 此信息冗余
        if 'homepage' in kw_info.keys():
            kw_info.pop('homepage')  

        write_row = dict(**p, **kw_info)
        HandleStore().write_detail_row(partner_file, write_row)


def extract_domain(url):
    if url.startswith(r'http://www.'):
        return True, url.replace(r'http://www.', '')

    if url.startswith(r'https://www.'):
        return True, url.replace(r'https://www.', '')

    if url.startswith(r'http://'):
        return True, url.replace(r'http://', '')

    if url.startswith(r'https://'):
        return True, url.replace(r'https://', '')

    return False, None


def help():
    print('%s [--fetch | --parse]' % sys.argv[0])


sys.excepthook = my_excepthook

if __name__ == '__main__':

    # 初始化log配置
    init_log_config()
    logging.info("=================================================")

    try:
        cmd = '--detail'
        if len(sys.argv) > 1:
            cmd = sys.argv[1]

        if cmd == '--fetch':
            # 提取web信息，存储在本地磁盘中
            spider_data()
        elif cmd == '--detail':
            # 解析每个web的详细信息，存储在对应的csv文件中
            parse_detail()
        else:
            # 输出帮助信息
            help()

    except getopt.GetoptError as e:
        logging.exception('get opt error')

    except Exception as e:
        logging.exception('exception happened')

    finally:
        logging.info("run over !!! ~~~~~~~~~~~~~~~~~~~~~")
