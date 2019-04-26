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

DETAIL_FILE = 'detail.csv'
SUMMARY_FILE = 'summary.csv'

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

EXT_DETAIL_KW_ARR = [
    'email', 'email_suffix', 'homepage', 'profile_domains', 'company',
    'user_name', 'phone', 'profile_owner', 'last_month_pv', 'last_month_events',
    'heatmap_count', 'page_group_count', 'conversion_count', 'login_last_30days',
    'total_kw_hits', 'distinct_kw_hits'
]

# 详细表展示关键词
SHOW_DETAIL_KW_ARR = EXT_DETAIL_KW_ARR + ALL_KW_ARR

EXT_SUMMARY_KW_ARR = [
    'label', 'email_suffix', 'homepage', 'email_count',
    'company', 'last_month_pv', 'last_month_events', 'heatmap_count',
    'page_group_count', 'conversion_count', 'login_last_30days',
    'total_kw_hits', 'distinct_kw_hits'
]

# 汇总表展示关键词
SHOW_SUMMARY_KW_ARR = EXT_SUMMARY_KW_ARR + ALL_KW_ARR


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
        # execute(['scrapy', 'crawl', 'quotes'])
        execute(['scrapy', 'crawl', 'partner'])    
    except Exception as e:
        logging.error(e)


@myutil.singleton
class HandleStore(object):

    def __init__(self):
        self.detail_fields = SHOW_DETAIL_KW_ARR
        self.summary_fields = SHOW_SUMMARY_KW_ARR
        make_dir('record')

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
                fieldnames=self.detail_fields
            )
            writer.writeheader()

    def write_summary_head(self, file_name, mode='w'):
        with open(file_name, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(
                f,
                dialect=myutil.MyDialect,
                fieldnames=self.summary_fields
            )
            writer.writeheader()

    def write_detail_row(self, file_name, row, mode='a'):
        with open(file_name, mode, newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(
                f,
                dialect=myutil.MyDialect,
                fieldnames=self.detail_fields
            )
            writer.writerow(row)

    def write_summary_row(self, file_name, row, mode='a'):
        with open(file_name, mode, newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(
                f,
                dialect=myutil.MyDialect,
                fieldnames=self.summary_fields
            )
            writer.writerow(row)


@myutil.singleton
class LoadSQLData(object):
    def __init__(self):
        self.clear()
        self.load_data()

    def clear(self):
        self.ref_site_status = {}
        self.user_login_log = {}
        self.usrelationship = {}
        self.user_detail = []
        self.dd = {}
        self.email2uid = {}

    def load_data(self):
        # 加载user detail表信息
        with open(myutil.SAVE_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.email2uid[row['email']] = int(row['uid'])
                self.user_detail.append(row)

        # 加载dd已经算好的csv数据
        dd_file = os.path.join('record', 'dd.csv')
        with open(dd_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.dd[row['email']] = row

        # 以下数据从产品数据库中加载
        db_info = myutil.get_db_info()
        conn = pymysql.connect(
            host=db_info['host'],
            port=db_info['port'],
            user=db_info['user'],
            password=db_info['passwd'],
            # db=MYSQL_DB
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        # 用户对应的所有档案
        with conn.cursor() as cursor:
            sql = '''
                select uid, siteid, type from ptmind_common.ptengine_usrelationship
                where uid is not null;
            '''
            cursor.execute(sql)
            for row in cursor:
                uid = row['uid']
                siteid = row['siteid']
                ptype = row['type']
                if uid in self.usrelationship.keys():
                    self.usrelationship[uid].append((siteid, ptype))
                else:
                    self.usrelationship[uid] = [(siteid, ptype)]

        # 档案状态表
        with conn.cursor() as cursor:
            sql = '''
                SELECT seqid, primary_domain FROM ptmind_common.ref_site_status
                where user_id is not null;
            '''
            cursor.execute(sql)
            for row in cursor:
                seqid = row['seqid']
                primary_domain = row['primary_domain'].strip()
                if seqid not in self.ref_site_status.keys():
                    self.ref_site_status[seqid] = primary_domain.split(r',')

        # 用户的登陆信息表
        # with conn.cursor() as cursor:
        #     db_name = 'ptmind_user_service'
        #     if myutil.get_run_type() == 'release':
        #         db_name = 'ptmind-user-service'

        #     time_before_30 = int((time.time() - 86500*30) * 1000)

        #     sql = '''
        #         select uid, login_time from %s.user_login_log
        #         where uid is not null and login_time > %s;
        #     ''' % (db_name, time_before_30)
        #     cursor.execute(sql)
        #     for row in cursor:
        #         user_id = row['uid']
        #         login_time = row['login_time']
        #         if user_id in self.user_login_log.keys():
        #             self.user_login_log[user_id].append(login_time)
        #         else:
        #             self.user_login_log[user_id] = [login_time]

        conn.close()

    def get_user_detail(self):
        return self.user_detail

    def is_profile_owner(self, uid):
        if uid in self.usrelationship.keys():
            for _siteid, ptype in self.usrelationship[uid]:
                if ptype == 0:
                    return 'Y'

        return 'N'

    def get_profile_domains(self, email, ftype='detail'):
        uid = self.__get_uid_by_email(email)
        if uid is None:
            return None

        all_profilee_domains = []

        if uid not in self.usrelationship.keys():
            return None

        for site_id, ptype in self.usrelationship[uid]:
            if ftype == 'detail' and ptype != 0:
                continue

            if site_id in self.ref_site_status.keys():
                all_profilee_domains += self.ref_site_status[site_id]

        return ','.join(all_profilee_domains)

    def __get_uid_by_email(self, email):
        if email in self.email2uid.keys():
            return self.email2uid[email]
        return None

    def get_dd_info(self, email):
        if email in self.dd.keys():
            info = self.dd[email]
            return {
                'last_month_pv': info['pv_cnt_m_prev'],
                'last_month_events': info['event_cnt_m_prev'],
                'heatmap_count': info['hm_page_cnt'],
                'page_group_count': info['pg_cnt'],
                'conversion_count': info['cv_cnt'],
                'login_last_30days': info['login_cnt_30d']
            }

        return None

    def get_primary_domain(self, site_id):
        if site_id in self.ref_site_status.keys():
            return self.ref_site_status[site_id]

        return 'N'

    def get_login_info(self, user_id):
        login_list = self.__get_login_time_list(user_id)
        if login_list is None:
            return 'N', 'N'

        login_list.sort()
        login_list = [int(one/1000) for one in login_list]
        new_login_list = list(map(self.__format_login_time, login_list))

        return new_login_list[-1], len(new_login_list)

    def __format_login_time(self, login_time):
        l_time = time.localtime(login_time)
        return time.strftime("%Y-%m-%d %H:%M:%S", l_time)

    def __get_login_time_list(self, user_id):
        if user_id in self.user_login_log.keys():
            return self.user_login_log[user_id]

        return None


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
        domain_dirs_path = os.path.join('.', 'data', '*', '*')
        domain_dirs = glob.glob(domain_dirs_path)
        for data_path in domain_dirs:
            
            basename = os.path.basename(data_path)
            if self.__is_cache_parse(basename):
                logging.info('ParseSpiderData cache: %s', data_path)
                continue
            
            logging.info('ParseSpiderData parse: %s', data_path)

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

                        # 排除不是本域名的url
                        if self.__is_cache_parse(json_email_suffix):
                            continue

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
        save_file = os.path.join('record', 'parse_spider.json')
        with open(save_file, 'a', newline='', encoding='utf-8') as f:
            json.dump(sum_kw_dict, f, ensure_ascii=False)
            f.write(os.linesep)

    def __load_parse(self):
        save_file = os.path.join('record', 'parse_spider.json')
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
    
    # fix bug: miss homepage
    domain2url = {}
    with open(myutil.DETECT_URL, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            domain2url[row['domain']] = row['url'].strip().rstrip(r'/')
    # end fix bug

    detail_file = HandleStore().handle_file_name(DETAIL_FILE)
    HandleStore().write_detail_head(detail_file)

    serial_line = 0
    user_list = LoadSQLData().get_user_detail()
    for user_row in user_list:
        email = user_row['email']

        serial_line += 1
        logging.info(
            'parse_data: serial num %d, email %s', serial_line, email
        )

        domain = myutil.get_domain(email)
        kw_info = ParseSpiderData().get_info(domain)
        if kw_info is None:
            continue
        
        # fix bug: miss homepage
        if len(kw_info['homepage']) == 0:
            kw_info['homepage'] = domain2url[domain]   

        profile_domains = LoadSQLData().get_profile_domains(email)
        profile_owner = LoadSQLData().is_profile_owner(int(user_row['uid']))

        user_db_info = {
            'email': email,
            'company': user_row['account_company_name'],
            'user_name': user_row['account_name'],
            'phone': user_row['telephone'],
            'profile_domains': profile_domains,
            'profile_owner': profile_owner
        }

        dd_info = LoadSQLData().get_dd_info(email)
        if dd_info is None:
            dd_info = {}

        detail_row = dict(**kw_info, **user_db_info, **dd_info)
        HandleStore().write_detail_row(detail_file, detail_row)


def parse_summary():
    summary_row_list = []
    emails_merge_dict = {}

    detail_file = os.path.join('record', DETAIL_FILE)

    # 统计合并相同后缀的email
    with open(detail_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for detail_row in reader:
            email_suffix = detail_row['email_suffix']
            if email_suffix in emails_merge_dict.keys():
                emails_merge_dict[email_suffix] += 1
            else:
                emails_merge_dict[email_suffix] = 1

    merge_email_suffix_list = []
    with open(detail_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for detail_row in reader:
            email_suffix = detail_row['email_suffix']
            if email_suffix in merge_email_suffix_list:
                continue

            merge_email_suffix_list.append(email_suffix)

            emails_merge_count = 1
            if email_suffix in emails_merge_dict.keys():
                emails_merge_count = emails_merge_dict[email_suffix]

            ext_summary = {
                'email_suffix': email_suffix,
                'homepage':  detail_row['homepage'],
                'email_count': emails_merge_count,
                'company': detail_row['company'],
                'last_month_pv': detail_row['last_month_pv'],
                'last_month_events': detail_row['last_month_events'],
                'heatmap_count': detail_row['heatmap_count'],
                'page_group_count': detail_row['page_group_count'],
                'conversion_count': detail_row['conversion_count'],
                'login_last_30days': detail_row['login_last_30days'],
                'total_kw_hits': detail_row['total_kw_hits'],
                'distinct_kw_hits': detail_row['distinct_kw_hits']
            }
            kw_summary = {}
            for kw in ALL_KW_ARR:
                kw_summary[kw] = detail_row[kw]

            summary_row = dict(**ext_summary, **kw_summary)
            summary_row_list.append(summary_row)

    summary_file = HandleStore().handle_file_name(SUMMARY_FILE)
    HandleStore().write_summary_head(summary_file)
    for summary_row in summary_row_list:
        HandleStore().write_summary_row(summary_file, summary_row)


def help():
    print('%s [--fetch | --parse | --summary]' % sys.argv[0])


sys.excepthook = my_excepthook

if __name__ == '__main__':

    # 初始化log配置
    init_log_config()
    logging.info("=================================================")

    try:
        cmd = '--fetch'
        if len(sys.argv) > 1:
            cmd = sys.argv[1]

        if cmd == '--fetch':
            # 提取web信息，存储在本地磁盘中
            spider_data()
        elif cmd == '--detail':
            # 解析每个web的详细信息，存储在对应的csv文件中
            parse_detail()
        elif cmd == '--summary':
            # 将所有的web信息进行汇总
            parse_summary()
        else:
            # 输出帮助信息
            help()

        # opts, _args = getopt.getopt(
        #     sys.argv[1:],
        #     "h",
        #     ["help", "fetch", "parse", "summary"]
        # )

        # if len(opts) == 0:
        #     help()
        #     raise ArgsException('no args')

        # for opt, arg in opts:
        #     if opt in ('-h', '--help'):
        #         # 输出帮助信息
        #         help()
        #     elif opt == '--fetch':
        #         # 提取web信息，存储在本地磁盘中
        #         parse_email()
        #     elif opt == '--parse':
        #         # 解析每个web的详细信息，存储在对应的csv文件中
        #         parse_data()
        #     elif opt == '--summary':
        #         # 将所有的web信息进行汇总
        #         collect_summary()

    except getopt.GetoptError as e:
        logging.error(e)

    except Exception as e:
        logging.exception('exception happened')

    finally:
        logging.info("run over !!! ~~~~~~~~~~~~~~~~~~~~~")
