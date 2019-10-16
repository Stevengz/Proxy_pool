from  Proxy_pool.crawler import Crawler
from  Proxy_pool.db import MySqlClient
from  Proxy_pool.setting import *
import sys

class Getter():
    def __init__(self):
        self.mysql = MySqlClient()
        self.crawler = Crawler()

    def is_over_threshold(self):
        """
        判断是否达到了代理池限制
        """
        if self.mysql.count() >= POOL_UPPER_THRESHOLD:
            return True
        else:
            return False
    
    def run(self):
        print('获取器开始执行')
        if not self.is_over_threshold():
            for callback_label in range(self.crawler.__CrawlFuncCount__):
                callback = self.crawler.__CrawlFunc__[callback_label]
                # 获取代理
                all_ip = self.crawler.get_proxies(callback)
                sys.stdout.flush()
                for ip in all_ip:
                    self.mysql.add(ip)
