import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import html
import re
import pymysql
import logging
import colorlog
from datetime import datetime
from typing import List, Dict

# 本地数据库配置（根据实际情况修改）
db_config = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "root",
    "db": "news_db",
    "charset": "utf8mb4"
}

# 配置彩色日志
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'red',
        'ERROR': 'purple',
        'CRITICAL': 'red,bg_white',
    }
))
logger = colorlog.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# 预编译正则表达式
date_pattern = re.compile(r'(\d{4}[-/]?\d{1,2}[-/]?\d{1,2}|\d{1,2}[-/]?\d{1,2}[-/]?\d{4}|\d{4}年\d{1,2}月\d{1,2}日|\d{2}\d{4}/\d{2}|\d{2}\d{4})')
chinese_pattern = re.compile(r'[\u4e00-\u9fa5]')
digit_pattern = re.compile(r'\d')

# 日期格式列表
date_formats = ['%Y年%m月%d日', '%Y%m%d', '%Y/%m/%d', '%Y-%m-%d', '%Y-%m', '%m%d%Y', '%d%Y/%m']

# 新闻类别映射
category_mapping = {
    "招生": ["招生", "录取", "admissions"],
    "科研": ["实验", "发现", "突破", "discovery"],
    "校园": ["校园", "校园生活", "campus"],
    "大学": ["大学", "学院", "university"],
    "学术": ["研究", "学术", "论文", "science", "research"],
    "活动": ["活动", "讲座", "会议", "event", "forum"],
    "公告": ["通知", "公告", "声明", "announcement"]
}

def categorize_news(title: str) -> str:
    """基于标题关键词自动分类"""
    title_lower = title.lower()
    for category, keywords in category_mapping.items():
        if any(kw in title_lower for kw in keywords):
            return category
    return "其他"

def check_url_availability(url: str, timeout: int = 5) -> bool:
    """检测URL可达性（使用HEAD方法）"""
    try:
        resp = requests.head(url, timeout=timeout, allow_redirects=True)
        return resp.status_code == 200
    except (requests.exceptions.RequestException, ValueError):
        return False

def clean_invalid_links() -> int:
    """清理无效链接记录"""
    try:
        with pymysql.connect(**db_config) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT id, link FROM news_info")
                records = cursor.fetchall()
                
                deleted_count = 0
                for record_id, link in records:
                    if not check_url_availability(link):
                        cursor.execute("DELETE FROM news_info WHERE id = %s", (record_id,))
                        deleted_count += 1
                        logger.warning(f"删除无效链接：{link[:50]}...")
                
                connection.commit()
                logger.info(f"清理完成，共删除{deleted_count}条记录")
                return deleted_count
    except Exception as e:
        logger.error(f"清理失败：{str(e)}")
        return 0

def parse_date(date_str: str) -> datetime:
    """多种日期格式解析"""
    for fmt in date_formats:
        try:
            date_obj = datetime.strptime(date_str, fmt)
            if fmt == '%Y-%m':  # 处理没有日期的格式
                date_obj = date_obj.replace(day=1)
            return date_obj
        except ValueError:
            continue
    raise ValueError("无法识别的日期格式")

def create_news_table() -> bool:
    """创建/更新新闻信息表结构"""
    try:
        with pymysql.connect(**db_config) as connection:
            with connection.cursor() as cursor:
                # 创建新表结构
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS news_info (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        university_name VARCHAR(255) NOT NULL COMMENT '大学名称',
                        title VARCHAR(191) UNIQUE NOT NULL COMMENT '新闻标题',
                        date DATE NOT NULL COMMENT '新闻日期',
                        link VARCHAR(255) NOT NULL COMMENT '新闻链接',
                        category VARCHAR(50) COMMENT '新闻分类',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                ''')

                # 添加category字段（如果不存在）
                cursor.execute("SHOW COLUMNS FROM news_info LIKE 'category'")
                if not cursor.fetchone():
                    cursor.execute("ALTER TABLE news_info ADD COLUMN category VARCHAR(50) AFTER link")
                    
                connection.commit()
                logger.info("表结构验证/更新成功")

                # 字段完整性检查
                cursor.execute("DESC news_info")
                field_list = [column[0] for column in cursor.fetchall()]
                required_fields = ['university_name', 'title', 'date', 'link', 'category']
                if all(field in field_list for field in required_fields):
                    logger.info("表字段验证通过")
                    return True
                logger.error("表字段缺失")
                return False
    except Exception as e:
        logger.error(f"表初始化失败: {str(e)}")
        return False

def insert_news_data(data: List[Dict]) -> int:
    """插入有效数据到数据库"""
    inserted_count = 0
    try:
        with pymysql.connect(**db_config) as connection:
            with connection.cursor() as cursor:
                insert_query = """
                    INSERT INTO news_info 
                    (university_name, title, date, link, category)
                    VALUES (%s, %s, %s, %s, %s)
                """

                for item in data:
                    # URL有效性检查
                    if not check_url_availability(item['url']):
                        logger.warning(f"链接不可达：{item['url'][:50]}...")
                        continue

                    # 构建插入数据
                    try:
                        cursor.execute(insert_query, (
                            item['university_name'],
                            item['title'],
                            item['date'],
                            item['url'],
                            categorize_news(item['title'])
                        ))
                        inserted_count += 1
                    except pymysql.IntegrityError as e:
                        if e.args[0] == 1062:
                            logger.debug(f"重复标题：{item['title'][:20]}...")
                        else:
                            raise
                
                connection.commit()
                logger.info(f"成功插入{inserted_count}条新记录")
                return inserted_count
    except Exception as e:
        logger.error(f"数据库操作失败: {str(e)}")
        return 0

def extract_news(html_content: str, base_url: str) -> List[Dict]:
    """从HTML中提取新闻数据"""
    soup = BeautifulSoup(html_content, 'html.parser')
    news_list = []
    
    for link in soup.find_all('a', href=True):
        # 提取标题
        title = html.unescape(link.get('title') or link.get_text(strip=True))
        if not title or len(title) < 8:
            continue

        # 过滤非中文标题
        if not chinese_pattern.search(title):
            continue

        # 处理链接
        url = urljoin(base_url, link['href'])
        
        # 查找日期
        element = link
        date_str = None
        while element and not date_str:
            text = element.get_text()
            match = date_pattern.search(text)
            if match:
                date_str = match.group(1)
            element = element.parent

        # 验证数据有效性
        if date_str and url:
            try:
                news_list.append({
                    "title": title,
                    "url": url,
                    "date": parse_date(date_str).strftime('%Y-%m-%d')
                })
            except ValueError:
                logger.warning(f"无效日期格式：{date_str}")
                
    return news_list

sites = [
    ("上海大学", "https://news.shu.edu.cn/index/zyxw.htm"),
    ("复旦大学", "https://www.fudan.edu.cn/"),
    ("上海交通大学", "https://news.sjtu.edu.cn/index.html"),
    ("华东师范大学","https://www.ecnu.edu.cn/xwlm/xwrd.htm"),
    ("同济大学", "https://news.tongji.edu.cn/tjyw1.htm"),
    ("上海师范大学", "https://xw.shnu.edu.cn/main.htm"),
    ("上海对外经贸大学", "https://news.suibe.edu.cn/main.htm"),
    ("上海交通大学", "https://news.sjtu.edu.cn/jdyw/index.html"),
    ("上海交通大学医学院附属新华医院", "https://www.xinhuamed.com.cn/news/index.html"),
    ("上海应用技术大学", "https://www.sit.edu.cn/xww/")
]

def main():
    if not create_news_table():
        logger.error("❌ 表初始化失败，退出程序")
        return

    logger.info("🚀 开始抓取新闻数据...")
    for uni_name, url in sites:
        try:
            logger.info(f"正在处理：{uni_name}")
            resp = requests.get(url, timeout=10)
            resp.encoding = 'utf-8'
            
            if not resp.ok:
                logger.warning(f"请求失败：{resp.status_code}")
                continue

            news_items = extract_news(resp.text, url.rsplit('/', 1)[0])
            formatted_data = [{
                "university_name": uni_name,
                "title": n['title'],
                "date": n['date'],
                "url": n['url']
            } for n in news_items]

            inserted = insert_news_data(formatted_data)
            logger.info(f"{uni_name} 插入{inserted}条数据")

        except Exception as e:
            logger.error(f"{uni_name} 处理失败：{str(e)[:200]}")

    logger.info("🧹 执行链接清理...")
    clean_invalid_links()
    logger.info("✅ 所有任务完成")

if __name__ == "__main__":
    main()