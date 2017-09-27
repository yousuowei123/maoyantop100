# auth: c_tob

import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import json
from multiprocessing import Pool
import csv
import pymongo
from config import *
import pymysql


client = pymongo.MongoClient(MONGO_URL, connect=False)
db = client[MONGO_DB]

# f = open('maoyan.csv', 'w', encoding='gb18030', newline='')
# writer = csv.DictWriter(f, fieldnames=['index','image','score','title','star','time'])
# writer.writeheader()


def create_table():
    try:
        conn = pymysql.connect(IP, user=USER, passwd=PASSWD, db=DB, charset='utf8')
        cur = conn.cursor()
        cur.execute('set names utf8')
        SQL = "CREATE TABLE IF NOT EXISTS maoyantop (id BIGINT PRIMARY KEY AUTO_INCREMENT,\
                rank VARCHAR(20), image VARCHAR(150), title VARCHAR(50), star VARCHAR(100), time VARCHAR(50),\
                score VARCHAR(10), INDEX(title))"
        print(SQL)
        cur.execute(SQL)

        cur.close()
        conn.close()
    except Exception as e:
        print(e)


def get_one_page(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        return r.text
    except RequestException:
        print('获取网页失败')
        return None


def parse_one_page(html):
    soup = BeautifulSoup(html, 'lxml')
    content = soup.find_all('dd')
    if content:
        for data in content:
            index = data.select('i')[0].get_text()
            image = data.select('img')[1].get('data-src')
            title = data.select('.name')[0].get_text()
            star = data.select('.star')[0].get_text().strip()[3:]
            time = data.select('.releasetime')[0].get_text().strip()[5:]
            integer = data.select('.integer')[0].get_text()
            fra = data.select('.fraction')[0].get_text()
            score = integer + fra
            yield {
                'rank': index,
                'image': image,
                'title': title,
                'star': star,
                'time': time,
                'score': score
            }

# 存储到txt中
# def write_to_txt(content):
#     with open('maoyan.txt', 'a', encoding='utf-8') as f:
#         f.write(json.dumps(content, ensure_ascii=False) + '\n')


# 存储到csv中
# def store_to_csv(content):
#         writer.writerow(content)


def save_to_mongo(content):
    try:
        if db[MONGO_TABLE].insert(content):
            print('写入到mongodb成功')
    except Exception as e:
        print(e)
        pass


def save_to_mysql(item):
    if item:
        try:
            ROWstr = ''
            COLstr = ''
            conn = pymysql.connect(IP, user=USER, passwd=PASSWD, db=DB, charset='utf8')
            cur = conn.cursor()
            cur.execute('set names utf8')

            for key in item.keys():
                COLstr = (COLstr + '"%s"' + ',') % key
                ROWstr = (ROWstr + '"%s"' + ',') % item.get(key)
            COLstr = COLstr.replace("\"", "")
            SQL = "INSERT INTO %s(%s) VALUES (%s)" % ("maoyantop", COLstr[:-1], ROWstr[:-1])
            print('Mysql正在存储', SQL)

            cur.execute(SQL)
            cur.connection.commit()
            cur.close()
            conn.close()

        except Exception as e:
            print(e)
            pass

def main(offset):
    url = 'http://maoyan.com/board/4?offset=' + str(offset)
    html = get_one_page(url)
    for item in parse_one_page(html):
        print(item)
        # write_to_txt(item)  # 存储到txt文件中
        # store_to_csv(item)  # 存储到csv文件中
        # save_to_mongo(item)  # 存储到mongodb中
        save_to_mysql(item)


if __name__ == "__main__":
    create_table()
    for i in range(10):
        main(i*10)
    # pool = Pool()
    # pool.map(main, [i*10 for i in range(10)])
    # pool.close()
    # pool.join()
    # f.close()
