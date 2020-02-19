import re
import os
import csv
import sys
import json
import time
import random
from datetime import datetime
import requests

from main import (
    spider_full_content,
    spider_user_info,
    save_columns_to_csv,
)

CSV_FILE_PATH = './temp_sina_topic.csv'
s = requests.Session()
weibo_count = 0


def prepare_csv():
    if not os.path.exists(CSV_FILE_PATH):
        fields_names = [
            'wb-id',
            'latest update',
            'user-id',
            'user-name',
            'user-rank',
            'content',
            'forward',
            'comment',
            'like',
            'created at',
            'relative time'
        ]
        with open(CSV_FILE_PATH, 'w', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields_names)
            writer.writeheader()

def scrawl_topic(min_since_id):
    """
        爬取新浪话题
        新浪微博分页机制：根据时间分页，每一条微博都有一个since_id，时间越大的since_id越大
        所以在请求时将since_id传入，则会加载对应话题下比此since_id小的微博，然后又重新获取最小since_id
        将最小since_id传入，依次请求，这样便实现分页
        :return:
        """

    global weibo_count
    # 1、构造请求
    topic_url = 'https://m.weibo.cn/api/container/getIndex?jumpfrom=weibocom&containerid=1008084882401a015244a2ab18ee43f7772d6f_-_feed'

    '''
    !!! important this should be change to the id of the newest post, check:https://juejin.im/post/5d46adfae51d456201486dcd on how to get this id
    '''
    if not min_since_id:
        min_since_id = '4470931667569516'

    topic_url = f"{topic_url}&since_id={min_since_id}"
    kv = {'user-agent': 'Mozilla/5.0',
          'Referer': 'https://m.weibo.cn/p/1008087a8941058aaf4df5147042ce104568da/super_index?jumpfrom=weibocom'}

    try:
        r = s.get(url=topic_url, headers=kv)
        r.raise_for_status()
    except:
        print('爬取失败')
        return

    # 2、解析数据
    r_json = json.loads(r.text)
    cards = r_json['data']['cards']
    # 2.1、第一次请求cards包含微博和头部信息，以后请求返回只有微博信息
    card_group = cards[2]['card_group'] if len(cards) > 1 else cards[0]['card_group']
    for card in card_group:
        # 创建保存数据的列表，最后将它写入csv文件
        sina_columns = []
        mblog = card['mblog']

        # 2.2、解析微博内容
        r_since_id = mblog['id']
        # 过滤html标签，留下内容
        sys.stdout.write(f'\rscrawling weibo {r_since_id}, total number of weibo scraped: {weibo_count}')
        sys.stdout.flush()
        sina_text = spider_full_content(r_since_id)
        weibo_count += 1

        # 2.3、解析用户信息
        user = mblog['user']
        # GET USER NAME, ID
        user_name = user['screen_name']
        user_id = user['id']
        user_rank = user['urank']

        now = datetime.now()
        timestamp = datetime.timestamp(now)

        # 把信息放入列表
        post_dict = {
            "wb-id": r_since_id,
            "user-id": user_id,
            "user-name": user_name,
            "user-rank": user_rank,
            "content": sina_text,
            "forward": mblog['reposts_count'],
            "comment": mblog['comments_count'],
            "like": mblog['attitudes_count'],
            "created at": timestamp,
            "relative time": mblog['created_at']
        }

        # 检验列表中信息是否完整
        # sina_columns数据格式：['wb-id', 'user-id', 'user-name', 'user-rank', 'content', 'forward', 'comment', 'like', 'created at', 'relative time']
        # 3、保存数据
        save_columns_to_csv(post_dict.values())

        # 4、获得最小since_id，下次请求使用
        if min_since_id:
            min_since_id = r_since_id if min_since_id > r_since_id else min_since_id
        else:
            min_since_id = r_since_id

        # 5、爬取用户信息不能太频繁，所以设置一个时间间隔
        time.sleep(random.randint(3, 6))


def pipeline():
    prepare_csv()
    for i in range(1000):
        print('Start scrawling page', i + 1)
        scrawl_topic(None)


if __name__ == '__main__':
    pipeline()