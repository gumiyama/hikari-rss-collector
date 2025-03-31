import re
import feedparser
import sys
import urllib.request
import html
import sqlite3
import os
from datetime import datetime, timedelta
import yfinance as yf

def create_database(db_path):
    """データベーステーブルの作成（存在しない場合）"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 報告書テーブルの作成（新たに報告日終値と現在価格の列を追加）
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        link TEXT UNIQUE,
        published TEXT,
        published_date TIMESTAMP,
        ticker TEXT,
        company TEXT,
        percentage REAL,
        percentage_change TEXT,
        percentage_change_value REAL,
        percentage_change_direction INTEGER,
        change_flag TEXT,
        report_date TEXT,
        report_date_timestamp TIMESTAMP,
        reason TEXT,
        reason_type INTEGER,
        purpose TEXT,
        report_date_close REAL,   -- 報告日終値
        current_price REAL,       -- 現在価格
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        year INTEGER,
        month INTEGER,
        day INTEGER
    )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker ON reports(ticker)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_percentage_change ON reports(percentage_change_direction)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_report_date ON reports(report_date_timestamp)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_year_month ON reports(year, month)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_reason_type ON reports(reason_type)')
    
    conn.commit()
    return conn

def parse_date(date_str):
    """日付文字列をパースしてタイムスタンプに変換"""
    try:
        formats = [
            '%a, %d %b %Y %H:%M:%S %z',
            '%Y年%m月%d日'
        ]
        for fmt in formats:
            try:
                date_obj = datetime.strptime(date_str, fmt)
                return date_obj
            except ValueError:
                continue
        return None
    except Exception:
        return None

def get_date_components(date_obj):
    """日付オブジェクトから年、月、日を取得"""
    if date_obj:
        return date_obj.year, date_obj.month, date_obj.day
    return None, None, None

def categorize_reason(reason_text):
    """提出事由をカテゴリ化する"""
    if not reason_text:
        return 0  # 不明
    categories = {
        '株券等保有割合が1%以上増加': 1,
        '株券等保有割合が1%以上減少': 2,
        '新規': 3,
        '変更': 4,
        '訂正': 5,
        '基準日': 6,
    }
    for key, value in categories.items():
        if key in reason_text:
            return value
    return 0

def extract_ticker_number(text):
    """テキストから証券コード（4桁の数字）を抽出"""
    match = re.search(r'\[(\d{4})\]', text)
    if match:
        return match.group(1)
    return None

def extract_percentage(text):
    """パーセンテージを抽出"""
    match = re.search(r'(\d+\.\d+)%', text)
    if match:
        return float(match.group(1))
    return None

def extract_percentage_change(text):
    """パーセンテージの変化を抽出し、値と方向を返す
    対応フォーマット:
      - 1.02pt↑ (増加)
      - 2.03pt↓ (減少)
      - Δ28.74pt (減少、デルタ記号付き)
    """
    if not text:
        return None, 0
    up_match = re.search(r'([\d\.]+)pt↑', text)
    if up_match:
        return float(up_match.group(1)), 1
    down_match = re.search(r'([\d\.]+)pt↓', text)
    if down_match:
        return float(down_match.group(1)), -1
    delta_match = re.search(r'[Δ△]([\d\.]+)pt', text)
    if delta_match:
        return float(delta_match.group(1)), -1
    simple_match = re.search(r'([\d\.]+)pt', text)
    if simple_match:
        return float(simple_match.group(1)), 0
    return None, 0

def fetch_prices(ticker, report_date_timestamp):
    """
    yfinance を用いて、指定した銘柄と報告日から
    報告日の終値と現在の価格を取得する。
    ※ 銘柄は、.Tがない場合は自動で付与（日本株の場合）
    """
    try:
        yf_ticker = yf.Ticker(ticker if "." in ticker else ticker + ".T")
        # report_date_timestamp を日付に変換
        report_date = report_date_timestamp.date()
        start_date = report_date.strftime("%Y-%m-%d")
        # 終値取得のため、翌日までの期間を指定
        end_date = (report_date + timedelta(days=1)).strftime("%Y-%m-%d")
        hist = yf_ticker.history(start=start_date, end=end_date)
        if not hist.empty:
            report_date_close = hist.iloc[0]['Close']
        else:
            report_date_close = None
        current_price = yf_ticker.info.get("regularMarketPrice", None)
        return report_date_close, current_price
    except Exception as e:
        print(f"Error fetching prices for ticker {ticker}: {e}")
        return None, None

def parse_and_store_rss(url, db_path):
    """RSSフィードをパースしてSQLiteに保存"""
    conn = create_database(db_path)
    cursor = conn.cursor()
    try:
        response = urllib.request.urlopen(url)
        rss_data = response.read().decode('utf-8')
        feed = feedparser.parse(rss_data)
        inserted_count = 0
        updated_count = 0
        if feed.bozo:
            print(f"Error parsing RSS feed: {feed.bozo_exception}")
            return None
        for entry in feed.entries:
            title = entry.title
            link = entry.link
            published = entry.published
            published_date = parse_date(published)
            
            cursor.execute("SELECT id FROM reports WHERE link = ?", (link,))
            existing = cursor.fetchone()
            
            description = entry.description if hasattr(entry, 'description') else ""
            description = html.unescape(description)
            description = re.sub(r'<[^>]+>', '', description)
            
            pub_year, pub_month, pub_day = get_date_components(published_date)
            
            data = {
                'title': title,
                'link': link,
                'published': published,
                'published_date': published_date,
                'ticker': None,
                'company': None,
                'percentage': None,
                'percentage_change': None,
                'percentage_change_value': None,
                'percentage_change_direction': 0,
                'change_flag': None,
                'report_date': None,
                'report_date_timestamp': None,
                'reason': None,
                'reason_type': 0,
                'purpose': None,
                'report_date_close': None,  # 新規
                'current_price': None,       # 新規
                'year': pub_year,
                'month': pub_month,
                'day': pub_day
            }
            
            # 【銘柄】などのラベルに基づいて内容を抽出
            content_pairs = re.findall(r'【(.*?)】(.*?)(?=【|$)', description, re.DOTALL)
            for key, value in content_pairs:
                cleaned_value = re.sub(r'\s+', ' ', value).strip()
                if key == '銘柄':
                    data['ticker'] = extract_ticker_number(cleaned_value)
                    company_match = re.search(r'\]\s*(.*?)$', cleaned_value)
                    if company_match:
                        data['company'] = company_match.group(1).strip()
                elif key == '割合':
                    data['percentage'] = extract_percentage(cleaned_value)
                    value_change, direction = extract_percentage_change(cleaned_value)
                    data['percentage_change_value'] = value_change
                    data['percentage_change_direction'] = direction
                elif key == '報告義務発生日':
                    data['report_date'] = cleaned_value
                    report_date_obj = parse_date(cleaned_value)
                    data['report_date_timestamp'] = report_date_obj
                    if report_date_obj and not data['year']:
                        data['year'], data['month'], data['day'] = get_date_components(report_date_obj)
                elif key == '提出事由':
                    data['reason'] = cleaned_value
                    data['reason_type'] = categorize_reason(cleaned_value)
                elif key == '保有目的':
                    data['purpose'] = cleaned_value
            
            # 銘柄と報告日の情報があれば、yfinanceで価格を取得
            if data['ticker'] and data['report_date_timestamp']:
                rd_close, curr_price = fetch_prices(data['ticker'], data['report_date_timestamp'])
                data['report_date_close'] = rd_close
                data['current_price'] = curr_price
            
            if existing:
                cursor.execute('''
                    UPDATE reports SET 
                        title = ?, published = ?, published_date = ?, ticker = ?, 
                        company = ?, percentage = ?, percentage_change_value = ?,
                        percentage_change_direction = ?, report_date = ?, 
                        report_date_timestamp = ?, reason = ?, reason_type = ?,
                        purpose = ?, report_date_close = ?, current_price = ?,
                        year = ?, month = ?, day = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE link = ?
                ''', (
                    data['title'], data['published'], data['published_date'], data['ticker'],
                    data['company'], data['percentage'], data['percentage_change_value'],
                    data['percentage_change_direction'], data['report_date'], 
                    data['report_date_timestamp'], data['reason'], data['reason_type'],
                    data['purpose'], data['report_date_close'], data['current_price'],
                    data['year'], data['month'], data['day'], link
                ))
                updated_count += 1
            else:
                cursor.execute('''
                    INSERT INTO reports (
                        title, link, published, published_date, ticker, 
                        company, percentage, percentage_change_value, percentage_change_direction,
                        report_date, report_date_timestamp, reason, reason_type, 
                        purpose, report_date_close, current_price, year, month, day
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    data['title'], data['link'], data['published'], data['published_date'], data['ticker'],
                    data['company'], data['percentage'], data['percentage_change_value'], 
                    data['percentage_change_direction'], data['report_date'], data['report_date_timestamp'],
                    data['reason'], data['reason_type'], data['purpose'], data['report_date_close'], data['current_price'],
                    data['year'], data['month'], data['day']
                ))
                inserted_count += 1
            
            conn.commit()
            
            print(f"Title: {title}")
            print(f"Link: {link}")
            for k, v in data.items():
                if v and k not in ['title', 'link']:
                    print(f"{k}: {v}")
            print("-" * 20)
        
        print(f"処理完了: {inserted_count}件追加, {updated_count}件更新")
    
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    
    finally:
        conn.close()

if __name__ == "__main__":
    # スクリプトと同じディレクトリにDBファイルを作成
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = sys.argv[2] if len(sys.argv) > 2 else os.path.join(script_dir, "reports.db")
    
    # コマンドライン引数がある場合は、そのURLのみを処理
    if len(sys.argv) > 1:
        feed_urls = [sys.argv[1]]
    else:
        feed_urls = [
            "https://ufocatch.com/a8/Rss/Filer/E35239",
            "https://ufocatch.com/a8/Rss/Filer/E36104",
            "https://ufocatch.com/a8/Rss/Filer/E11852",
            "https://ufocatch.com/a8/Rss/Filer/E34138",
            "https://ufocatch.com/a8/Rss/Filer/E31883",
            "https://ufocatch.com/a8/Rss/Filer/E11852",
            "https://ufocatch.com/a8/Rss/Filer/E08827"
        ]
    
    # 重複除外し、順序を保ったままユニーク化
    unique_feed_urls = []
    seen = set()
    for url in feed_urls:
        if url not in seen:
            unique_feed_urls.append(url)
            seen.add(url)
    
    print(f"データベースファイル: {db_path}")
    for url in unique_feed_urls:
        print(f"Processing feed: {url}")
        parse_and_store_rss(url, db_path)
