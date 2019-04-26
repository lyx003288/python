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


sys.excepthook = my_excepthook

if __name__ == '__main__':
    # 初始化工作路径
    os.chdir(G_CUR_FILE_PATH)

    # 初始化log配置
    init_log_config()
    logging.info("=================================================")

    data_file = os.path.join('D:', 'data', 'db_data.csv')
    data = {}
    count = 0
    with open(data_file, 'r+', encoding='utf-8') as f:
        csv_file = csv.reader(f)
        for line in csv_file:
            email = line[5].strip('"').strip()
            user_name = line[7].strip('"').strip()
            company_name = line[8].strip('"').strip()
            phone = line[14].strip('"').strip()
            record = [email, user_name, company_name, phone]
            info = email.split('@')
            if len(info) < 2:
                continue

            # print('emial=',email,',user_name=',user_name,'com_name=',company_name,'phthon=', phone)
            count += 1
            if int(count % 3000) == 0:
                print('the count is %s' % count)

            domain = info[1]
            data[domain] = record

    class MyDialect(csv.Dialect):
        delimiter = ','
        lineterminator = os.linesep
        quoting = csv.QUOTE_NONNUMERIC
        doublequote = True
        quotechar = '"'
        escapechar = '\\'

    count = 0
    with open('D:\\data\\result.csv', 'a+', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, dialect=MyDialect, fieldnames=[
                                'url', 'email', 'uname', 'cname', 'ph'])

        url_file = os.path.join('D:', 'data', 'email.txt')
        with open(url_file, 'r', encoding='utf-8') as f:
            for line in f.readlines():
                count = count + 1
                print(count)
                line = line.strip()
                pos = line.find('.')
                if len(line) < pos+1:
                    continue

                domain = line[pos+1:]
                if domain in data:
                    one = data[domain]

                    w = {
                        'url': line,
                        'email': one[0] if len(one[0]) > 0 else 'N',
                        'uname': one[1] if len(one[1]) > 0 else 'N',
                        'cname': one[2] if len(one[2]) > 0 else 'N',
                        'ph': one[3] if len(one[3]) > 0 else 'N'
                    }
                    writer.writerow(w)
                else:
                    w = {
                        'url': line,
                        'email': 'N',
                        'uname': 'N',
                        'cname': 'N',
                        'ph': 'N'
                    }
                    writer.writerow(w)

    logging.info('run over !!!!!!!!!!!')
