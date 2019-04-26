import os
import csv
import hashlib
# from tutorial import settings as sets
import settings as sets

# +-------------------------+--------------+------+-----+---------+----------------+
# | Field                   | Type         | Null | Key | Default | Extra          |
# +-------------------------+--------------+------+-----+---------+----------------+
# | id                      | bigint(20)   | NO   | PRI | NULL    | auto_increment |
# | uid                     | bigint(20)   | NO   | MUL | NULL    |                |
# | email                   | varchar(255) | YES  | UNI | NULL    |                |
# | siteid                  | bigint(20)   | YES  |     | NULL    |                |
# | contact_address         | varchar(255) | YES  |     | NULL    |                |
# | contact_email           | varchar(255) | YES  |     | NULL    |                |
# | account_nickname        | varchar(255) | YES  |     | NULL    |                |
# | account_name            | varchar(255) | YES  |     | NULL    |                |
# | account_company_name    | varchar(500) | YES  |     | NULL    |                |
# | account_company_size    | varchar(255) | YES  |     | NULL    |                |
# | account_company_revenue | varchar(255) | YES  |     | NULL    |                |
# | know_channel            | varchar(255) | YES  |     | NULL    |                |
# | account_department      | varchar(500) | YES  |     | NULL    |                |
# | account_position        | varchar(255) | YES  |     | NULL    |                |
# | telephone               | varchar(255) | YES  |     | NULL    |                |
# | create_time             | bigint(20)   | NO   |     | NULL    |                |
# | last_modify_time        | bigint(20)   | NO   |     | NULL    |                |
# | version                 | int(11)      | YES  | MUL | NULL    |                |
# | source                  | int(11)      | YES  |     | NULL    |                |
# | source_detail           | varchar(255) | YES  |     | NULL    |                |
# | code                    | varchar(255) | YES  |     | NULL    |                |
# | channel_source          | varchar(255) | YES  |     | NULL    |                |
# | utm_source              | varchar(255) | YES  |     | NULL    |                |
# | utm_medium              | varchar(255) | YES  |     | NULL    |                |
# | industry_category       | varchar(200) | YES  |     | NULL    |                |
# | website_url             | varchar(200) | YES  |     | NULL    |                |
# | analytics_chanllenge    | varchar(255) | YES  |     | NULL    |                |
# | right_change            | int(11)      | NO   |     | 0       |                |
# | urltitle                | varchar(20)  | YES  |     | NULL    |                |
# | resolution              | varchar(100) | YES  |     | NULL    |                |
# | device                  | varchar(100) | YES  |     | NULL    |                |
# | show_event_first        | int(11)      | YES  |     | 0       |                |
# | cj_source_detail        | varchar(255) | YES  |     | NULL    |                |
# | is_activated            | int(11)      | YES  |     | 1       |                |
# | language                | varchar(20)  | YES  |     | NULL    |                |
# | before_sale             | varchar(100) | YES  |     | NULL    |                |
# | after_sale              | varchar(100) | YES  |     | NULL    |                |
# | affiliate_campaign_id   | bigint(20)   | YES  |     | NULL    |                |
# | promotion_id            | bigint(20)   | YES  |     | NULL    |                |
# | data_version            | varchar(10)  | YES  |     | v3      |                |
# +-------------------------+--------------+------+-----+---------+----------------+
# 40 rows in set (0.10 sec)
TABLE_USER_DETAIL = 'ptengine_user_detail'
FIELDS_USER_DETAIL = [
    "id", "uid", "email", "siteid", "contact_address", "contact_email",
    "account_nickname", "account_name", "account_company_name", "account_company_size",
    "account_company_revenue", "know_channel", "account_department", "account_position",
    "telephone", "create_time", "last_modify_time", "version", "source",
    "source_detail", "code", "channel_source", "utm_source", "utm_medium",
    "industry_category", "website_url", "analytics_chanllenge", "right_change",
    "urltitle", "resolution", "device", "show_event_first", "cj_source_detail",
    "is_activated", "language", "before_sale", "after_sale", "affiliate_campaign_id",
    "promotion_id", "data_version"
]

SAVE_FILE = os.path.join('record', 'mysql_data.csv')
DETECT_URL = os.path.join('record', 'detect_url.csv')

CSV_SPLIT_CHAR = ','            # SOURCE_FILE文件分隔符


def make_dir(name):
    if not os.path.exists(name):
        os.mkdir(name)


def get_sdir(flag):
    flag_md5 = hashlib.md5(flag.encode('utf-8')).hexdigest()
    return str(int(flag_md5, 16) % 100).strip()


# 使用装饰器(decorator),这是一种更pythonic,更elegant的方法
def singleton(cls):
    instances = {}
    from functools import wraps

    @wraps(cls)
    def _singleton(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    return _singleton


class MyDialect(csv.Dialect):
    delimiter = ','
    lineterminator = os.linesep
    quoting = csv.QUOTE_NONNUMERIC
    doublequote = True
    quotechar = '"'
    escapechar = '\\'


def get_domain(email):
    domain = ''
    split_list = email.split('@')
    if len(split_list) == 2:
        domain = split_list[1]
    return domain.strip().lower().lstrip('/')


def get_rid_last_slash(path):
    if len(path) > 0 and path[-1:] == '/':
        return path[:-1]
    return path

def get_run_type():
    return sets.RUN_TYPE

def get_urls_limit():
    return sets.RUN_URL_LIMIT

# 测试区：mysql -uptmind -pptmind2012 -hptconftestdb.ptfuture.cn/172.20.3.66 -P23306
# 正式区：mysql -hjpptcommondb.ptengine.com -u Ptreadonly --port 13307 -pPtMind123qwe -A
def get_db_info():
    if sets.RUN_TYPE == 'release':
        MYSQL_HOST = 'jpptcommondb.ptengine.com'
        MYSQL_PORT = 13307
        MYSQL_USER = 'Ptreadonly'
        MYSQL_PASSWD = 'PtMind123qwe'
        # MYSQL_DB = 'ptmind_common'
    else:
        # MYSQL_HOST = 'ptconftestdb.ptfuture.cn'
        MYSQL_HOST = '172.20.3.66'
        MYSQL_PORT = 23306
        MYSQL_USER = 'ptmind'
        MYSQL_PASSWD = 'ptmind2012'
        # MYSQL_DB = 'ptmind_common'

    return {
        'host': MYSQL_HOST,
        'port': MYSQL_PORT,
        'user': MYSQL_USER,
        'passwd': MYSQL_PASSWD
    }