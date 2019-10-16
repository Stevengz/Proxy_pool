import pymysql
from Proxy_pool.error import PoolEmptyError
from Proxy_pool.setting import *
from random import choice
import re


class MySqlClient(object):
    # 初始化
    def __init__(self, host=HOST, port=MYSQL_PORT, username=MYSQL_USERNAME, password=MYSQL_PASSWORD, sqlname=SQL_NAME):
        self.db = pymysql.connect(host=host, user=username, password=password, port=port, db=sqlname)
        self.cursor = self.db.cursor()

    # 添加代理IP
    def add(self, ip, score=INITIAL_SCORE):
        sql_add = "INSERT INTO PROXY (IP,SCORE) VALUES ('%s', %s)" % (ip, score)
        if not re.match('\d+\.\d+\.\d+\.\d+\:\d+', ip):
            print('代理不符合规范', ip, '丢弃')
            return
        if not self.exists(ip):
            self.cursor.execute(sql_add)
            self.db.commit()
            return True

    # 减少代理分数
    def decrease(self, ip):
        sql_get = "SELECT * FROM PROXY WHERE IP='%s'" % (ip)
        self.cursor.execute(sql_get)
        score = self.cursor.fetchone()[1]
        if score and score > MIN_SCORE:
            print('代理', ip, '当前分数', score, '减 1')
            sql_change = "UPDATE PROXY SET SCORE = %s WHERE IP = '%s'" % (score-1, ip)
        else:
            print('代理', ip, '当前分数', score, '移除')
            sql_change = "DELETE FROM PROXY WHERE IP = %s" % (ip)
        self.cursor.execute(sql_change)
        self.db.commit()

    # 分数最大化
    def max(self, ip):
        print('代理', ip, '可用，设置为', MAX_SCORE)
        sql_max = "UPDATE PROXY SET SCORE = %s WHERE IP = '%s'" % (MAX_SCORE, ip)
        self.cursor.execute(sql_max)
        self.db.commit()
        
    # 随机获取有效代理
    def random(self):
        # 先从满分中随机选一个
        sql_max = "SELECT * FROM PROXY WHERE SCORE=%s" % (MAX_SCORE)
        if self.cursor.execute(sql_max):
            results = self.cursor.fetchall()
            return choice(results)[0]
        # 没有满分则随机选一个
        else:
            sql_all = "SELECT * FROM PROXY WHERE SCORE BETWEEN %s AND %s" % (MIN_SCORE, MAX_SCORE)
            if self.cursor.execute(sql_all):
                results = self.cursor.fetchall()
                return choice(results)[0]
            else:
                raise PoolEmptyError

    # 判断是否存在
    def exists(self, ip):
        sql_exists = "SELECT 1 FROM PROXY WHERE IP='%s' limit 1" % ip
        return self.cursor.execute(sql_exists)
        
    # 获取数量
    def count(self):
        sql_count = "SELECT * FROM PROXY"
        return self.cursor.execute(sql_count)

    # 获取全部
    def all(self):
        self.count()
        return self.cursor.fetchall()

    # 批量获取
    def batch(self, start, stop):
        sql_batch = "SELECT * FROM PROXY LIMIT %s, %s" % (start, stop - start)
        self.cursor.execute(sql_batch)
        return self.cursor.fetchall()
