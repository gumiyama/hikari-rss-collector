import re
import feedparser
import sys
import urllib.request
import html
import sqlite3
import os
from datetime import datetime

def create_database(db_path):
    """データベーステーブルの作成（存在しない場合）"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 報告書テーブルの作成
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
        percentage_change TEXT,               -- 元の変化量テキスト（1.02pt↑, Δ28.74ptなど）
        percentage_change_value REAL,         -- 変化量の数値（例: 1.02）
        percentage_change_direction INTEGER,  -- 変化の方向: 1=増加, -1=減少, 0=変化なし/不明
        change_flag TEXT,                     -- 人間が読みやすい増減フラグ: "増", "減", NULL
        report_date TEXT,
        report_date_timestamp TIMESTAMP,      -- 報告義務発生日のタイムスタンプ
        reason TEXT,
        reason_type INTEGER,                  -- 理由タイプ（コード化）
        purpose TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        -- インデックス作成に役立つカラム
        year INTEGER,                         -- 年
        month INTEGER,                        -- 月
        day INTEGER                           -- 日
    )
    ''')
    
    # インデックスの作成
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
        # RSS日付形式（例: Wed, 19 Feb 2025 16:57:00 +0900）をパース
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
    
    # カテゴリマッピング
    categories = {
        '株券等保有割合が1%以上増加': 1,  # 1%以上増加
        '株券等保有割合が1%以上減少': 2,  # 1%以上減少
        '新規': 3,                      # 新規報告
        '変更': 4,                      # その他の変更
        '訂正': 5,                      # 訂正報告
        '基準日': 6,                    # 基準日の変更
    }
    
    for key, value in categories.items():
        if key in reason_text:
            return value
    
    return 0  # 一致するものがない場合

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
    
    # ↑マークがある場合（増加）
    up_match = re.search(r'([\d\.]+)pt↑', text)
    if up_match:
        return float(up_match.group(1)), 1
    
    # ↓マークがある場合（減少）
    down_match = re.search(r'([\d\.]+)pt↓', text)
    if down_match:
        return float(down_match.group(1)), -1
    
    # Δ記号がある場合（減少）- ギリシャ文字デルタまたは三角形の記号
    delta_match = re.search(r'[Δ△]([\d\.]+)pt', text)
    if delta_match:
        return float(delta_match.group(1)), -1
    
    # 数値とptのみの場合（記号なしの変化）
    simple_match = re.search(r'([\d\.]+)pt', text)
    if simple_match:
        return float(simple_match.group(1)), 0
    
    # 変化なしの場合や別の形式の場合
    return None, 0

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
        else:
            for entry in feed.entries:
                title = entry.title
                link = entry.link
                published = entry.published
                published_date = parse_date(published)
                
                # まずリンクで既存のエントリを検索
                cursor.execute("SELECT id FROM reports WHERE link = ?", (link,))
                existing = cursor.fetchone()
                
                # 説明からデータを抽出
                description = entry.description if hasattr(entry, 'description') else ""
                description = html.unescape(description)
                description = re.sub(r'<[^>]+>', '', description)
                
                # 公開日からコンポーネントを取得
                pub_year, pub_month, pub_day = get_date_components(published_date)
                
                # データを保持する辞書
                data = {
                    'title': title,
                    'link': link,
                    'published': published,
                    'published_date': published_date,
                    'ticker': None,
                    'company': None,
                    'percentage': None,
                    'percentage_change': None,        # 元のテキスト形式の変化量
                    'percentage_change_value': None,  # 数値
                    'percentage_change_direction': 0, # 増減方向
                    'change_flag': None,              # 人間が読みやすい増減フラグ
                    'report_date': None,
                    'report_date_timestamp': None,
                    'reason': None,
                    'reason_type': 0,  # デフォルトは未分類
                    'purpose': None,
                    'year': pub_year,
                    'month': pub_month,
                    'day': pub_day
                }
                
                # 内容を抽出
                content_pairs = re.findall(r'【(.*?)】(.*?)(?=【|$)', description, re.DOTALL)
                
                for key, value in content_pairs:
                    cleaned_value = re.sub(r'\s+', ' ', value).strip()
                    
                    if key == '銘柄':
                        data['ticker'] = extract_ticker_number(cleaned_value)
                        # 銘柄名を取得（証券コードの後ろ）
                        company_match = re.search(r'\]\s*(.*?)$', cleaned_value)
                        if company_match:
                            data['company'] = company_match.group(1).strip()
                    elif key == '割合':
                        data['percentage'] = extract_percentage(cleaned_value)
                        value, direction = extract_percentage_change(cleaned_value)
                        data['percentage_change_value'] = value
                        data['percentage_change_direction'] = direction
                    elif key == '報告義務発生日':
                        data['report_date'] = cleaned_value
                        # 報告日をタイムスタンプに変換
                        report_date_obj = parse_date(cleaned_value)
                        data['report_date_timestamp'] = report_date_obj
                        # 報告日の年月日を取得
                        if report_date_obj and not data['year']:  # 公開日がない場合は報告日を使用
                            data['year'], data['month'], data['day'] = get_date_components(report_date_obj)
                    elif key == '提出事由':
                        data['reason'] = cleaned_value
                        data['reason_type'] = categorize_reason(cleaned_value)
                    elif key == '保有目的':
                        data['purpose'] = cleaned_value
                
                # レコードの挿入または更新
                if existing:
                    # 既存レコードを更新
                    cursor.execute('''
                    UPDATE reports SET 
                        title = ?, published = ?, published_date = ?, ticker = ?, 
                        company = ?, percentage = ?, percentage_change_value = ?,
                        percentage_change_direction = ?, report_date = ?, 
                        report_date_timestamp = ?, reason = ?, reason_type = ?,
                        purpose = ?, year = ?, month = ?, day = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE link = ?
                    ''', (
                        data['title'], data['published'], data['published_date'], data['ticker'],
                        data['company'], data['percentage'], data['percentage_change_value'],
                        data['percentage_change_direction'], data['report_date'], 
                        data['report_date_timestamp'], data['reason'], data['reason_type'],
                        data['purpose'], data['year'], data['month'], data['day'], link
                    ))
                    updated_count += 1
                else:
                    # 新規レコードを挿入
                    cursor.execute('''
                    INSERT INTO reports (
                        title, link, published, published_date, ticker, 
                        company, percentage, percentage_change_value, percentage_change_direction,
                        report_date, report_date_timestamp, reason, reason_type, 
                        purpose, year, month, day
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        data['title'], data['link'], data['published'], data['published_date'], data['ticker'],
                        data['company'], data['percentage'], data['percentage_change_value'], 
                        data['percentage_change_direction'], data['report_date'], data['report_date_timestamp'],
                        data['reason'], data['reason_type'], data['purpose'], 
                        data['year'], data['month'], data['day']
                    ))
                    inserted_count += 1
                
                # 変更をコミット
                conn.commit()
                
                # 処理されたエントリの詳細を出力
                print(f"Title: {title}")
                print(f"Link: {link}")
                
                for key, value in data.items():
                    if value and key not in ['title', 'link']:
                        print(f"{key}: {value}")
                        
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
    url = sys.argv[1] if len(sys.argv) > 1 else "https://ufocatch.com/a8/Rss/Filer/E35239"
    
    # スクリプトと同じディレクトリにDBファイルを作成
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = sys.argv[2] if len(sys.argv) > 2 else os.path.join(script_dir, "reports.db")
    
    print(f"データベースファイルを作成: {db_path}")
    parse_and_store_rss(url, db_path)