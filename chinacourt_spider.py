# -*- coding: utf-8 -*-
# Python version: Python 3.4.3
__author__ = 'zy'


import re
import time
import random
import requests

from scrapy.http import HtmlResponse
from scrapy.selector import HtmlXPathSelector
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


def get_total_page():
    url = 'http://www.live.chinacourt.org/chat/more/type/1.shtml'
    try:
        html = requests.get(url).content
        resp = HtmlResponse(url, body=html)
        resp = HtmlXPathSelector(resp)
        last_page_url = resp.select('//a[contains(@href,"page")]/@href').extract()[-1]
        total_page_num = int(re.findall('\d+', last_page_url)[-1])

        return total_page_num

    except requests.RequestException as e:
        print(e)
        return

##res = get_total_page()


def parse_one_page(url):
    html = requests.get(url).content
    resp = HtmlResponse(url, body=html)
    resp = HtmlXPathSelector(resp)
    doc_urls = resp.select('//div[@class="titlegc"]/a/@href').extract()
    text_time = resp.select('//div[@class="text_time"]/text()').re('\d+-\d+-\d+')

    assert len(doc_urls) == len(text_time)


    return list(zip(doc_urls, text_time))

##res = parse_one_page('http://www.live.chinacourt.org/chat/more/type/1.shtml')


def split_list(lst, n):
    """将列表n等分"""
    return [lst[i:i+n] for i in range(len(lst)) if i % n == 0]

##l1 = [0, 1, 2, 3, 4, 5]
##l2 = [0, 1, 2, 3, 4, 5, 6]
##print(split_list(l1, 2))
##print(split_list(l2, 3))


def pretty_rightname(right_name):
    """规范化发言人名字"""
    if right_name == '':
        return ''
    return re.findall('\w+', right_name)[0]


def get_one_chunk_conversation(selector_chunk):
    """分时段获取发言内容"""
    conversation_chunk = selector_chunk.select('.//text()').extract()
    conversation_chunk = ''.join([ele.strip() for ele in conversation_chunk])

    return re.sub('\s+', '', conversation_chunk)


def get_page_content(url):
    """解析每篇文章内容"""
    html = requests.get(url)
    sleep_time = random.random()
    time.sleep(sleep_time)
    resp = HtmlResponse(url, body=html.content)
    resp = HtmlXPathSelector(resp)

    doc = {'url': url}
    try:
        title = resp.select('//div[@class="font20 bold mg_8 line_bot"]/text()').extract()[0].strip()
        doc['title'] = title
    except:
        pass

    chat_content = []

    for ele in resp.select('//li'):
        conversation_chunk = get_one_chunk_conversation(ele)  # 分时间段截取内容
        chat_content.append(conversation_chunk)

    doc['zhibo_content'] = chat_content

    return doc

####url = 'http://www.live.chinacourt.org/chat/chat/2014/04/id/35691.shtml'  # 没有报错信息
##url = 'http://www.live.chinacourt.org/chat/chat/2014/04/id/35557.shtml'  # rec
##res = get_page_content(url)
##print(res)


def fill_mising_chatcontent():
    """有些document的chat_content为[],此函数为这些document重新尝试下载"""

    mongo_client = MongoClient('192.168.86.108', 27017)
    db = mongo_client.chinacourt
    coll_chatrecords = db.chat_records

    for doc in coll_chatrecords.find({'chat_content': []}):
        url = doc['url']
        _id = doc['_id']
        print(url)

        try:
            rec = get_page_content(url)  # 解析文本
            chat_content = rec['chat_content']
            try:
                if chat_content:
                    coll_chatrecords.update({'_id': _id}, {'$set': {'chat_content': chat_content}})
                    print('> The document [ {} ] is filled new data successfully!\n'.format(_id))
                    continue
            except:
                pass

        except:
            pass

    mongo_client.close()

    print('==== Done! ====')

##res = fill_mising_chatcontent()


def create_page_url_collection():
    """将文档的url保存到数据库中"""

    mongo_client = MongoClient('192.168.86.108', 27017)
    db = mongo_client.chinacourt
    coll_pageurls = db.page_urls

    start_page = 1
    end_page = get_total_page()
    print('Total {} pages to be download...\n'.format(end_page))

    doc_cnt = 0

    for page in range(start_page, end_page+1):
        print('========== The {}th page is downloading... ==========\n'.format(page))
        page_url = 'http://www.live.chinacourt.org/chat/more/type/1/page/{}.shtml'.format(page)

        try:
            urls_date = parse_one_page(page_url)
            for url, zhibo_time in urls_date:
                url = 'http://www.live.chinacourt.org{}'.format(url)
                if not coll_pageurls.find_one({'_id': url}):
                    doc = {'_id': url,
                           'url': url,
                           'zhibo_time': zhibo_time,
                           'is_processed': False  # 初始化时没下载
                    }
                    coll_pageurls.insert(doc)
                    doc_cnt += 1
                    print('> ({}th) New page url -> [{}]\n'.format(doc_cnt, url))
        except:
            pass

##res = create_page_url_collection()


def update_page_urls(repitive_times=100):
    """更新page_urls表"""

    mongo_client = MongoClient('192.168.86.108', 27017)
    db = mongo_client.chinacourt
    coll_pageurls = db.page_urls

    start_page = 1
    end_page = get_total_page()
    print('> updating collectin [ page_urls ] ...\n')
    print('Total {} pages to be download...\n'.format(end_page))

    doc_cnt = 0
    try_times = 0

    for page in range(start_page, end_page+1):
        print('========== The {}th page is downloading... ==========\n'.format(page))
        page_url = 'http://www.live.chinacourt.org/chat/more/type/1/page/{}.shtml'.format(page)

        try:
            urls_date = parse_one_page(page_url)
            for url, zhibo_time in urls_date:
                url = 'http://www.live.chinacourt.org{}'.format(url)
                if not coll_pageurls.find_one({'_id': url}):
                    doc = {'_id': url,
                           'url': url,
                           'zhibo_time': zhibo_time,
                           'is_processed': False  # 初始化时没下载
                    }
                    coll_pageurls.insert(doc)
                    doc_cnt += 1
                    print('>>> ({}th) New page url -> [{}]\n'.format(doc_cnt, url))
                else:
                    try_times += 1
                    if try_times > repitive_times:  # 如果url重复次数超过一定次数，终止，认为之后的url已采集
                        mongo_client.close()
                        print('>>> [Done!] update urls finished!\n')
                        return
                    print('>>> [existed! {}\{}] url -> {} !\n '.format(try_times, repitive_times, url))

        except:
            pass


def pipeline():
    """数据爬取"""
    mongo_client = MongoClient('192.168.86.108', 27017)
    db = mongo_client.chinacourt
    coll_chatrecords = db.chat_records
    coll_pageurls = db.page_urls

    download_urls = coll_pageurls.find({'is_processed': False})
    if download_urls.count() == 0:
        print('No pages need to download !\n')
        return

    total_download = download_urls.count()
    success_cnt = 0
    fail_count = 0

    for doc in download_urls:
        url = doc['_id']
        try:
            rec = get_page_content(url)  # 解析文本, [特别注意：] 如果要抓取的直播内容正在更新，则会chat_content=[], 需要直播完毕后，使用fill_mising_chatcontent()函数更新missing data
            rec['zhibo_date'] = doc['zhibo_time']
            doc_id = '_'.join([rec.get('zhibo_date'), rec.get('title')])
            rec['_id'] =  doc_id
            if rec.get('zhibo_content') == []:  # 内容为空，跳过该文档，以后再下载
                continue
            if not coll_chatrecords.find_one({'_id': doc_id}):
                coll_chatrecords.insert(rec)
                coll_pageurls.update({'_id': url}, {'$set': {'is_processed': True}})  # 标记已下载
                success_cnt += 1
                print('> [successful!][process: {}/{}]The document <{}> is download successfully!\n'.format(success_cnt, total_download, url))

        except Exception as e:
            fail_count += 1
            print('>>> [fail!][process: {}/{}] url -> {}\n'.format(fail_count, total_download, url))
            print(e)

    mongo_client.close()

    print('==== Done! ====')

##pipeline()


def main():
    try:
        print('> updating collectin [ page_urls ] ...\n')
        update_page_urls(30)
    except Exception as e:
        print(e)
    try:
        print('>>> Trying to update documents ...\n')
        pipeline()
    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()





