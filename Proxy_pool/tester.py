import asyncio
import aiohttp
import time
import sys
from aiohttp import ClientError
from  Proxy_pool.db import MySqlClient
from  Proxy_pool.setting import *


class Tester(object):
    def __init__(self):
        self.mysql = MySqlClient()
    
    async def test_single_ip(self, ip):
        """
        测试单个代理
        :param ip:
        :return:
        """
        conn = aiohttp.TCPConnector(verify_ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            try:
                if isinstance(ip, bytes):
                    ip = ip.decode('utf-8')
                real_ip = 'http://' + ip
                print('正在测试', ip)
                async with session.get(TEST_URL, proxy=real_ip, timeout=15, allow_redirects=False) as response:
                    if response.status in VALID_STATUS_CODES:
                        self.mysql.max(ip)
                        print('代理可用', ip)
                    else:
                        self.mysql.decrease(ip)
                        print('请求响应码不合法 ', response.status, 'IP', ip)
            except (ClientError, aiohttp.client_exceptions.ClientConnectorError, asyncio.TimeoutError, AttributeError):
                self.mysql.decrease(ip)
                print('代理请求失败', ip)
    
    def run(self):
        """
        测试主函数
        :return:
        """
        print('测试器开始运行')
        try:
            count = self.mysql.count()
            print('当前剩余', count, '个代理')
            for i in range(0, count, BATCH_TEST_SIZE):
                start = i
                stop = min(i + BATCH_TEST_SIZE, count)
                print('正在测试第', start + 1, '-', stop, '个代理')
                test_ip_group = self.mysql.batch(start, stop)
                loop = asyncio.get_event_loop()
                tasks = [self.test_single_ip(ip_tuple[0]) for ip_tuple in test_ip_group]
                loop.run_until_complete(asyncio.wait(tasks))
                sys.stdout.flush()
                time.sleep(5)
        except Exception as e:
            print('测试器发生错误', e.args)
