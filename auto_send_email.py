# -*- coding: utf-8 -*-
# Python version: Python 3.4.3


import time
import datetime
import traceback
import pymongo

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
import smtplib

from pymongo import MongoClient
import sys

reload(sys)
sys.setdefaultencoding("utf-8")

db_client = MongoClient("192.168.86.108",27017)

mailto_list = ["yaozhang@iflytek.com", "niansi@iflytek.com", "ybcui@iflytek.com", "lingwang3@iflytek.com"] # 邮件接收方的邮件地址
mail_host = "mail.iflytek.com"  # 邮件传送协议服务器
mail_user = "yaozhang@iflytek.com" # 邮件发送方的邮箱账号


def send_mail(to_list, sub, content, email_pwd='code#136'):
    """发送电子邮件.
    email_pwd: 邮件发送方的邮箱密码
    """
    me ="%s<"% Header("张耀","utf-8")+mail_user+">"
    #用html的格式发送表格
    msg = MIMEText(content, _subtype='html', _charset='utf-8')
    msg['Subject'] = sub  #邮件主题
    msg['From'] = me
    msg['To'] = ";".join(to_list)
    try:
        server = smtplib.SMTP()
        server.connect(mail_host)
        server.login(mail_user, email_pwd)
        server.sendmail(me, to_list, msg.as_string())
        server.close()
        return True
    except Exception as e:
        print(e)
        return False


def build_one_row(lst):
    """构建table的一行"""
    row = ''.join('<td style="width:100px;text-align:center;">{}</td>'.format(td) for td in lst)
    return '<tr>{}</tr>'.format(row)


def build_table(table_matrix):
    """构建HTNL表格"""
    table = ''.join(build_one_row(tr) for tr in table_matrix)
    return '<table border="1" cellspacing="0" bordercolor="#000000" style="text-align:center;border-collapse:collapse">{}</table>'.format(table)


def build_html(table_matrix):
    statistic_table = build_table(table_matrix)
    date_today = str(datetime.date.today())
    return """<html>
                   <p>统计日期：{statistic_date}</p>
                   {table}
              </html>""".format(statistic_date=date_today, table=statistic_table)


def get_statistic_sinian():
    """斯念的统计数据"""

    date = int(time.mktime(time.strptime(str(datetime.date.today()-datetime.timedelta(days=1)),'%Y-%m-%d')))
    new_video_num = db_client.niansi.douban_video_valid_url.find({'url':{'$exists':True},'create_time':{'$gt':date}}).batch_size(5).count()
    total_video_num = db_client.niansi.douban_video_valid_url.find({'url':{'$exists':True}}).batch_size(5).count()

    row_headers = ['豆瓣影视']  # 统计项
    category_increase = [str(new_video_num)]  # 统计项昨日增加数
    category_total = [str(total_video_num)]  # 统计项总数

    return list(zip(row_headers, category_increase, category_total))

##res = get_statistic_sinian()

def get_statistic_zy():
    """我的数据统计"""

    # time.struct_time(tm_year=2016, tm_mon=8, tm_mday=11, tm_hour=0, tm_min=0, tm_sec=0, tm_wday=3, tm_yday=224, tm_isdst=-1)
    yesterday_struct_time = time.strptime(str(datetime.date.today() - datetime.timedelta(days=1)), '%Y-%m-%d')
    yesterday_str = time.strftime('%Y-%m-%d %H:%M:%S', yesterday_struct_time)  # '2016-08-11 00:00:00'
    today_struct_time = time.strptime(str(datetime.date.today()), '%Y-%m-%d')
    today_str = time.strftime('%Y-%m-%d %H:%M:%S', today_struct_time)

    if not db_client.mtime_movies.log.find_one({'_id': 'last_statistic_time'}):  # 记录最近一次统计数据的日期
        db_client.mtime_movies.log.insert({'_id': 'last_statistic_time', 'last_statistic_time': today_str})

    last_statistic_time = db_client.mtime_movies.log.find_one({'_id': 'last_statistic_time'}).get('last_statistic_time')

    num_new_movie = db_client.mtime_movies.movieBasicInfo.find({'create_time': {'$gt': last_statistic_time}}).count()  # 新增电影数
    num_total_movie = db_client.mtime_movies.movieBasicInfo.find({}).count()  # 电影总数
    num_new_person = db_client.mtime_movies.peopleBasicInfo.find({'create_time': {'$gt': last_statistic_time}}).count()  # 新增影人数
    num_total_person = db_client.mtime_movies.peopleBasicInfo.find({}).count()  # 影人总数

    # 更新最近一次的统计日期
    db_client.mtime_movies.log.update({'_id': 'last_statistic_time'}, {'$set': {'last_statistic_time': today_str}})

    row_headers = ['时光网_电影', '时光网_影人']  # 统计项
    category_increase = [str(num_new_movie), str(num_new_person)]  # 统计项昨日增加数
    category_total = [str(num_total_movie), str(num_total_person)]  # 统计项总数

    return list(zip(row_headers, category_increase, category_total))

##res = get_statistic_zy()

if __name__ == '__main__':
##    print('python3 auto_email.py --email_pwd\n')
    print('start:',datetime.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S"))

    table_fields = [('统计项', '昨日新增', '目前总数')]
    table_matrix_sinian = get_statistic_sinian()
    table_matrix_zy = get_statistic_zy()
    table_matrix = table_fields + table_matrix_sinian + table_matrix_zy  # 统计数据

    sub = "影视资源下载统计"
    html = build_html(table_matrix)
    if send_mail(mailto_list, sub, html):
        print("发送成功!")
        print('end:',datetime.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S"))
    else:
        print("发送失败!")

