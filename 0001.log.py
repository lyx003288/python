#!/usr/bin/env python3 -u
# -*- coding: UTF-8 -*-


# ######################################################################################################################
# log模块
# ######################################################################################################################

import logging
import logging.handlers
import os
import sys

# 当前脚本所在路径
G_CUR_FILE_PATH = os.path.split(os.path.realpath(__file__))[0]


def make_dir(name):
    if not os.path.exists(name):
        os.makedirs(name)


def my_excepthook(exc_type, exc_value, traceback):
    logging.error(
        'Uncaught Exception',
        exc_info=(exc_type, exc_value, traceback)
    )


def init_log_config():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(filename)s %(funcName)s %(lineno)d] %(levelname)s: %(message)s',
        datefmt='[%m/%d %H:%M:%S]'
    )

    #################################################################################################
    formatter = logging.Formatter('%(asctime)s [%(filename)s %(funcName)s %(lineno)d] %(levelname)s: %(message)s')

    rotating = logging.handlers.RotatingFileHandler(
        "log/info.log",
        maxBytes=1024 * 1024 * 10,
        backupCount=3,
        delay=True
    )
    rotating.setLevel(logging.INFO)
    rotating.setFormatter(formatter)
    logging.getLogger().addHandler(rotating)

    error_rotating = logging.handlers.RotatingFileHandler(
        "log/error.log",
        maxBytes=1024 * 1024 * 10,
        backupCount=3,
        delay=True
    )
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

    # noinspection PyBroadException
    try:
        pass

    except Exception as _e:
        logging.exception('exception happened')

    finally:
        logging.info("%s run over !!!!!!!!!!!!!!!!!!!!!!!" % sys.argv)
