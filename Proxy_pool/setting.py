# 数据库地址
HOST = '127.0.0.1'
# MySql端口
MYSQL_PORT = 3306
# MySQl用户名、密码
MYSQL_USERNAME = 'root'
MYSQL_PASSWORD = 'steven1999'
# 数据库名
SQL_NAME = 'test'

# 代理等级
MAX_SCORE = 30
MIN_SCORE = 0
INITIAL_SCORE = 10

VALID_STATUS_CODES = [200, 302]

# 代理池数量界限
POOL_UPPER_THRESHOLD = 1000

# 检查周期
TESTER_CYCLE = 20
# 获取周期
GETTER_CYCLE = 300

# 测试API，建议抓哪个网站测哪个
TEST_URL = 'http://www.baidu.com'

# API配置
API_HOST = '0.0.0.0'
API_PORT = 5555

# 开关
TESTER_ENABLED = True
GETTER_ENABLED = True
API_ENABLED = True

# 最大批测试量
BATCH_TEST_SIZE = 30