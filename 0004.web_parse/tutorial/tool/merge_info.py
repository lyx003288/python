#!/usr/bin/env python3 -u
# -*- coding: utf-8 -*-

import logging
import logging.handlers
import os
import sys
import requests
import bs4
import time
import csv
from multiprocessing import Pool
from multiprocessing import cpu_count

# 当前脚本所在路径
G_CUR_FILE_PATH = os.path.split(os.path.realpath(__file__))[0]


class MyException(Exception):
    pass


def my_excepthook(exc_type, exc_value, traceback):
    logging.error("Uncaught Exception", exc_info=(
        exc_type, exc_value, traceback))


def make_dir(name):
    if not os.path.exists(name):
        os.mkdir(name)


def init_log_config():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(filename)s %(funcName)s %(lineno)d] %(levelname)s: %(message)s',
                        datefmt='[%m/%d %H:%M:%S]')

    #################################################################################################
    formatter = logging.Formatter(
        '%(asctime)s [%(filename)s %(funcName)s %(lineno)d] %(levelname)s: %(message)s')

    rotating = logging.handlers.RotatingFileHandler(
        "log/info.log", maxBytes=1024*1024*10, backupCount=3, delay=True)
    rotating.setLevel(logging.DEBUG)
    rotating.setFormatter(formatter)
    logging.getLogger().addHandler(rotating)

    error_rotating = logging.handlers.RotatingFileHandler(
        "log/error.log", maxBytes=1024*1024*10, backupCount=3, delay=True)
    error_rotating.setLevel(logging.WARNING)
    error_rotating.setFormatter(formatter)
    logging.getLogger().addHandler(error_rotating)

    make_dir("log")


class MyDialect(csv.Dialect):
    delimiter = ','
    lineterminator = os.linesep
    quoting = csv.QUOTE_NONNUMERIC
    doublequote = True
    quotechar = '"'
    escapechar = '\\'


class FieldError(Exception):
    pass


sys.excepthook = my_excepthook

if __name__ == '__main__':
    # 初始化工作路径
    os.chdir(G_CUR_FILE_PATH)

    # 初始化log配置
    init_log_config()
    logging.info("=================================================")

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
        'マーケター', 'デザイナー', 'ECサイト', 'インバウンドビジネス',
        'インバウンドマーケティング', 'a/b test', 'a/b テスト'
    ]
    KW_ARR = [key.lower() for key in KW_ARR]

    PARTNER_HEADER = ['签约的partner', 'URL']

    work_space = os.path.join(
        'C:\\', 'Users', 'ptmind', 'OneDrive', 'workspace', 'web_spider', 'tutorial', 'tutorial'
    )

    summary_file = os.path.join(work_space, 'record', 'summary.csv')
    partner_file = os.path.join(work_space, 'record', 'partner.csv')
    target_file = os.path.join(work_space, 'record', 'partner2.csv')

    summary_data = {}
    with open(summary_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            home_page = row['homepage'].strip().rstrip(r'/')
            if len(home_page) > 0:
                summary_data[home_page] = row

    if not os.path.exists(target_file):
        with open(target_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(
                f,
                dialect=MyDialect,
                fieldnames=PARTNER_HEADER + KW_ARR
            )
            writer.writeheader()

    with open(target_file, 'a', newline='', encoding='utf-8-sig') as target_f:
        writer = csv.DictWriter(
            target_f,
            dialect=MyDialect,
            fieldnames=PARTNER_HEADER + KW_ARR
        )

        with open(partner_file, 'r', newline='', encoding='utf-8-sig') as partner_f:
            reader = csv.DictReader(partner_f)
            for row in reader:
                url = row['URL'].strip().rstrip(r'/')
                if url[0:4] != 'http':
                    continue

                if url not in summary_data.keys():
                    raise MyException('url not exist')

                write_row = {}
                for kw in PARTNER_HEADER:
                    write_row[kw] = row[kw]

                summary_row = summary_data[url]
                for kw in KW_ARR:
                    write_row[kw] = summary_row[kw]

                writer.writerow(write_row)

    logging.info('run over !!!!!!!!!!!')
