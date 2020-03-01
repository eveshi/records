import re
import os
import csv
import json
import time
import random
import pymongo
from datetime import datetime
import sys

import requests
from constant import ncov_topic_url, not_ncov_topic_url
from html_process import process_html



# 生成Session对象，用于保存Cookie
s = requests.Session()

def login_sina():
    login_url = 'https://passport.weibo.cn/sso/login'
    headers = {'user-agent': 'Mozilla/5.0',
               'Referer': 'https://passport.weibo.cn/signin/login?entry=mweibo&res=wel&wm=3349&r=https%3A%2F%2Fm.weibo.cn%2F'}

    data = {'username': os.environ["USER"],
            'password': os.environ["PW"],
            'savestate': 1,
            'entry': 'mweibo',
            'mainpageflag': 1}
    try:
        r = s.post(login_url, headers=headers, data=data)
        r.raise_for_status()
    except:
        print('登录请求失败')
        return 0
    # 打印请求结果
    print(json.loads(r.text)['msg'])
    return 1


def spider_topic(post_collection, page, topic_url):
    '''
    topic do not have since_id, thus we only 30 pages a time
    '''
       
    topic_url = f"{topic_url}&page={page}"
    kv = {'user-agent': 'Mozilla/5.0',
          'Referer': 'https://m.weibo.cn/p/1008087a8941058aaf4df5147042ce104568da/super_index?jumpfrom=weibocom'}

    try:
        r = s.get(url=topic_url, headers=kv)
        r.raise_for_status()
    except:
        print('链接爬取失败')
        return False
    # 2、解析数据
    r_json = json.loads(r.text)
    cards = r_json['data']['cards']
    # 2.1、第一次请求cards包含微博和头部信息，以后请求返回只有微博信息
    duplicate_count = 0
    for card in cards:
        if duplicate_count >= 3:
            print("duplicate exceeds")
            return False

        if 'mblog' not in card:
            continue

        mblog = card['mblog']

        find_id = mblog['id']

        # 2.3、解析用户信息
        user = mblog['user']
        # GET USER NAME, ID
        user_name = user['screen_name']
        user_id = user['id']
        user_rank = user['urank']

        # GET forward, comment, like
        forward = mblog['reposts_count'] 
        comment = mblog['comments_count']
        like = mblog['attitudes_count']

        if "retweeted_status" in mblog:
            if mblog["retweeted_status"]["user"] is not None:
                find_id = mblog["retweeted_status"]["id"]
                user_name = mblog["retweeted_status"]["user"]["screen_name"]
                user_id = mblog["retweeted_status"]["user"]["id"]
                user_rank = mblog["retweeted_status"]["user"]['urank']
                forward = mblog["retweeted_status"]['reposts_count'] 
                comment = mblog["retweeted_status"]['comments_count']
                like = mblog["retweeted_status"]['attitudes_count']
            else:
                continue
        
        try:
            sina_text = spider_full_content(find_id)
        except:
            continue

        # 过滤html标签，留下内容

        now = datetime.now()
        timestamp = datetime.timestamp(now)

        try:
            # 把信息放入列表
            post_dict = {
            "wb-id": find_id,
            "user-id": user_id, 
            "user-name": user_name, 
            "user-rank": user_rank, 
            "clean_content": sina_text[1],
            "full_content": sina_text[0],
            "forward": forward, 
            "comment": comment, 
            "like": like, 
            "created at": timestamp, 
            "relative time": mblog['created_at']
            }
        except:
            continue

        # 检验列表中信息是否完整
        # sina_columns数据格式：['wb-id', 'user-id', 'user-name', 'user-rank', 'clean_content', 'full_content', 'forward', 'comment', 'like', 'created at', 'relative time']
        # 3、保存数据
        # if not exists, insert. if exists, skip.
        post_collection.update({'wb-id': find_id}, {'$setOnInsert': post_dict}, upsert=True)
        print(f"{find_id} created by {user_name} is finished.")

        # 5、爬取用户信息不能太频繁，所以设置一个时间间隔
        time.sleep(random.randint(1, 5))

def spider_full_content(id) -> list:
    """
    GET FULL CONTENT OF THE WEIBO
    """
    weibo_detail_url = f'https://m.weibo.cn/statuses/extend?id={id}'
    kv = {'user-agent': 'Mozilla/5.0'}
    try:
        r = s.get(url=weibo_detail_url, headers=kv)
        r.raise_for_status()
    except:
        print('爬取信息失败')
        return
    r_json = json.loads(r.text)
    weibo_full_content = r_json['data']['longTextContent']

    # TODO: maybe using MARKDOWN grammar to retain info
    clean_content = weibo_full_content
    if weibo_full_content.startswith('<a  href=', 0):
        clean_content = clean_content.split('</a>')[1]
    if len(weibo_full_content.split('<a data-url')) > 1:
        clean_content = clean_content.split('<a data-url')[0]

    # replace <br /> with \n
    # and extract <a> tag using regex
    clean_content = process_html(clean_content)

    return [weibo_full_content, clean_content]


def patch_topic(db, topic_url):
    if not login_sina():
      return

    for i in range(30):
        print('第%d页' % (i + 1))
        if not spider_topic(db, i, topic_url):
            continue
    
if __name__ == '__main__':

    # setup database
    client = pymongo.MongoClient(f"mongodb+srv://{os.environ['DB_USER']}:{os.environ['DB_PW']}@{os.environ['DB_HOST']}")
    db = client.weibo_topic

    raw_post = db.raw_post

    # patch topic for ncov patient
    patch_topic(raw_post, ncov_topic_url)

    not_conv_raw_post = db.not_conv_raw_post
    for url in not_ncov_topic_url:
        patch_topic(not_conv_raw_post, url)