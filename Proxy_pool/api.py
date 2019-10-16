from flask import Flask, g
from  Proxy_pool.db import MySqlClient

__all__ = ['app']

app = Flask(__name__)

def get_conn():
    if not hasattr(g, 'mysql'):
        g.mysql = MySqlClient()
    return g.mysql

@app.route('/')
def index():
    return '<h2>Welcome to Proxy Pool System</h2>'

@app.route('/random')
def get_proxy():
    """
    Get a proxy
    :return: 随机代理
    """
    conn = get_conn()
    return conn.random()

@app.route('/count')
def get_counts():
    """
    Get the count of proxies
    :return: 代理池总量
    """
    conn = get_conn()
    return str(conn.count())
