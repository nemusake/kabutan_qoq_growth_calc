#!/usr/bin/env python3
"""株探から四半期データを正確に取得してCSV出力するスクリプト"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
import re
from datetime import datetime, timedelta
from src.pdf_analyzer import download_pdf, extract_balance_sheet_data


def fetch_kabutan_page(code: str = "9984") -> Optional[str]:
    """株探の財務ページからHTMLを取得"""
    url = f"https://kabutan.jp/stock/finance?code={code}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"ページの取得に失敗しました: {e}")
        return None


def fetch_weekly_stock_data(code: str = "9984") -> List[Dict]:
    """株探の週足データを複数ページから取得"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    all_weekly_data = []
    
    # 複数ページからデータを取得（通常2ページ分で十分）
    for page in [1, 2]:
        url = f"https://kabutan.jp/stock/kabuka?code={code}&ashi=wek&page={page}"
        
        try:
            print(f"週足データページ{page}を取得中...")
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            html = response.text
            
            soup = BeautifulSoup(html, 'lxml')
            
            # 週足テーブルを探す（過去データ用: stock_kabuka_dwm）
            past_data_table = soup.find('table', class_='stock_kabuka_dwm')
            if past_data_table:
                print(f"  ページ{page}で過去週足テーブル(stock_kabuka_dwm)を発見")
                rows = past_data_table.find_all('tr')
                
                # ヘッダー行をスキップして株価データを処理
                for row in rows[1:]:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 8:  # 8列（日付、始値、高値、安値、終値、前週比、前週比％、売買高）
                        try:
                            date_text = cells[0].get_text().strip()
                            open_price_text = cells[1].get_text().strip()
                            
                            # デバッグ：日付テキストを表示（「今週」を含む場合は特別に表示）
                            if '今週' in date_text or '今週' in str(cells[0]):
                                print(f"  ★今週データを発見: '{date_text}', 始値テキスト: '{open_price_text}'")
                            else:
                                print(f"  日付テキスト: '{date_text}', 始値テキスト: '{open_price_text}'")
                            
                            # 日付のパース（例: 2024/12/27 または 25/08/12 形式）
                            if '/' in date_text and len(date_text.split('/')) == 3:
                                date_parts = date_text.split('/')
                                year = int(date_parts[0])
                                month = int(date_parts[1])
                                day = int(date_parts[2])
                                
                                # 2桁年の場合は2000年代として統一
                                if year < 100:
                                    year += 2000
                                
                                date_obj = datetime(year, month, day)
                                
                                # 始値のパース
                                open_price = parse_stock_price(open_price_text)
                                
                                if open_price is not None:
                                    all_weekly_data.append({
                                        '日付': date_obj,
                                        '始値': open_price
                                    })
                        except (ValueError, IndexError) as e:
                            # パースエラーは無視して続行
                            continue
            
            # 今週データ用テーブル（stock_kabuka0）も処理
            if page == 1:  # 今週データは最初のページにのみ存在
                current_week_table = soup.find('table', class_='stock_kabuka0')
                if current_week_table:
                    print(f"  ページ{page}で今週テーブル(stock_kabuka0)を発見")
                    rows = current_week_table.find_all('tr')
                    
                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 8:
                            try:
                                date_text = cells[0].get_text().strip()
                                open_price_text = cells[1].get_text().strip()
                                
                                print(f"  今週テーブル - 日付テキスト: '{date_text}', 始値テキスト: '{open_price_text}'")
                                
                                # 「今週」の場合は2列目が実際の日付の可能性
                                if '今週' in date_text and len(cells) >= 2:
                                    actual_date_text = cells[1].get_text().strip()
                                    if '/' in actual_date_text:
                                        date_text = actual_date_text
                                        open_price_text = cells[2].get_text().strip() if len(cells) >= 3 else open_price_text
                                        print(f"  今週データ修正: 日付='{date_text}', 始値='{open_price_text}'")
                                
                                # 日付のパース
                                if '/' in date_text and len(date_text.split('/')) == 3:
                                    date_parts = date_text.split('/')
                                    year = int(date_parts[0])
                                    month = int(date_parts[1])
                                    day = int(date_parts[2])
                                    
                                    # 2桁年の場合は2000年代として統一
                                    if year < 100:
                                        year += 2000
                                    
                                    date_obj = datetime(year, month, day)
                                    
                                    # 始値のパース
                                    open_price = parse_stock_price(open_price_text)
                                    
                                    if open_price is not None:
                                        all_weekly_data.append({
                                            '日付': date_obj,
                                            '始値': open_price
                                        })
                                        print(f"  今週データ追加成功: {date_obj.strftime('%Y/%m/%d')} - {open_price}円")
                            except (ValueError, IndexError) as e:
                                # パースエラーは無視して続行
                                continue
            
            # 従来のテーブル検索（フォールバック）
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                
                # ヘッダー行をチェック（日付、始値、高値、安値、終値などを含むテーブル）
                header_found = False
                for row in rows:
                    cells = row.find_all(['th', 'td'])
                    if cells:
                        header_text = ' '.join([cell.get_text().strip() for cell in cells])
                        if '日付' in header_text and '始値' in header_text:
                            header_found = True
                            break
                
                if header_found:
                    print(f"  ページ{page}で週足テーブルを発見")
                    print(f"  このテーブルの行数: {len(rows)}")
                    
                    # 全ての行を確認（「今週」を見つけるため）
                    for row_idx, row in enumerate(rows):  # ヘッダー行をスキップ
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 8:  # 8列（日付、始値、高値、安値、終値、前週比、前週比％、売買高）
                            try:
                                date_text = cells[0].get_text().strip()
                                open_price_text = cells[1].get_text().strip()
                                
                                # デバッグ：日付テキストを表示（「今週」を含む場合は特別に表示）
                                if '今週' in date_text or '今週' in str(cells[0]):
                                    print(f"  ★今週データを発見: '{date_text}', 始値テキスト: '{open_price_text}'")
                                else:
                                    print(f"  日付テキスト: '{date_text}', 始値テキスト: '{open_price_text}'")
                                
                                # 日付のパース（例: 2024/12/27 または 25/08/12 形式）
                                if '/' in date_text and len(date_text.split('/')) == 3:
                                    date_parts = date_text.split('/')
                                    year = int(date_parts[0])
                                    month = int(date_parts[1])
                                    day = int(date_parts[2])
                                    
                                    # 2桁年の場合は2000年代として統一
                                    if year < 100:
                                        year += 2000
                                    
                                    date_obj = datetime(year, month, day)
                                    
                                    # 始値のパース
                                    open_price = parse_stock_price(open_price_text)
                                    
                                    if open_price is not None:
                                        all_weekly_data.append({
                                            '日付': date_obj,
                                            '始値': open_price
                                        })
                            except (ValueError, IndexError) as e:
                                # パースエラーは無視して続行
                                continue
                    
                    break  # 最初に見つかったテーブルを使用
            
        except requests.RequestException as e:
            print(f"ページ{page}の週足データ取得に失敗: {e}")
            # エラーが発生してもpage1のデータがあれば続行
            if page == 1 and not all_weekly_data:
                return []
    
    # 重複を除去（同じ日付のデータがある場合は最初のものを使用）
    unique_data = {}
    for item in all_weekly_data:
        date_key = item['日付']
        if date_key not in unique_data:
            unique_data[date_key] = item
    
    weekly_data = list(unique_data.values())
    
    # 日付でソート（新しい順）
    weekly_data.sort(key=lambda x: x['日付'], reverse=True)
    
    print(f"合計週足データ {len(weekly_data)}件を取得")
    return weekly_data


def parse_stock_price(text: str) -> Optional[float]:
    """株価文字列をパース"""
    if not text or text == '-' or text == '－':
        return None
    
    # カンマを除去し、マイナス記号を正規化
    text = text.replace(',', '').replace('－', '-')
    
    try:
        return float(text)
    except ValueError:
        return None


def find_stock_price_after_announcement(announcement_date: str, weekly_data: List[Dict]) -> Dict[str, Optional[float]]:
    """発表日の翌日以降で最も近い株価データを取得、なければ発表日当日のデータを使用"""
    result = {
        '株価日付': None,
        '始値': None
    }
    
    if not announcement_date or not weekly_data:
        return result
    
    try:
        # 発表日をパース（例: "2024/12/27" または "24/11/10" 形式）
        if '/' in announcement_date:
            date_parts = announcement_date.split('/')
            if len(date_parts) == 3:
                year = int(date_parts[0])
                month = int(date_parts[1]) 
                day = int(date_parts[2])
                
                # 2桁年の場合は2000年代として統一
                if year < 100:
                    year += 2000
                
                announcement_datetime = datetime(year, month, day)
            else:
                return result
        else:
            return result
        
        # 発表日の翌日を計算
        target_date = announcement_datetime + timedelta(days=1)
        
        # 週足データから翌日以降で最も近い日付を探す
        best_match = None
        min_diff = float('inf')
        
        for stock_data in weekly_data:
            stock_date = stock_data['日付']
            
            # 発表日翌日以降のデータのみ対象
            if stock_date >= target_date:
                # より近い日付を優先
                diff = (stock_date - target_date).days
                if diff < min_diff:
                    min_diff = diff
                    best_match = stock_data
        
        # 翌日以降にデータがない場合、発表日当日のデータを探す
        if not best_match:
            for stock_data in weekly_data:
                stock_date = stock_data['日付']
                
                # 発表日当日のデータを探す
                if stock_date == announcement_datetime:
                    best_match = stock_data
                    break
        
        if best_match:
            result['株価日付'] = best_match['日付'].strftime('%Y/%m/%d')
            result['始値'] = best_match['始値']
        
    except (ValueError, IndexError, KeyError) as e:
        print(f"株価データマッチングエラー: {e}")
    
    return result


def calculate_stock_correlations(data_with_growth: List[Dict]) -> None:
    """最新3四半期の四半期成長率・経常益利回りと株価の相関を計算"""
    
    # データを決算期でソート（新しい順）
    sorted_data = sorted(data_with_growth, key=lambda x: x['決算期'], reverse=True)
    
    # 最新3四半期のデータを取得
    latest_3_quarters = sorted_data[:3]
    
    # 初期化：すべての行にNoneを設定
    for item in data_with_growth:
        item['四半期成長率株価相関'] = None
        item['経常益利回り株価相関'] = None
    
    # 四半期成長率と株価の相関計算
    growth_rates = []
    stock_prices_for_growth = []
    
    for quarter in latest_3_quarters:
        if quarter.get('四半期成長率') is not None and quarter.get('始値') is not None:
            growth_rates.append(quarter['四半期成長率'])
            stock_prices_for_growth.append(quarter['始値'])
    
    # 3つのデータが揃っている場合のみ相関を計算し、最新の四半期のみに設定
    if len(growth_rates) == 3 and len(stock_prices_for_growth) == 3:
        correlation_growth = np.corrcoef(growth_rates, stock_prices_for_growth)[0, 1]
        # 最新の四半期データ（sorted_data[0]）にのみ相関値を設定
        sorted_data[0]['四半期成長率株価相関'] = round(correlation_growth, 3)
    
    # 経常益利回りと株価の相関計算
    yields = []
    stock_prices_for_yield = []
    
    for quarter in latest_3_quarters:
        if quarter.get('経常益利回り') is not None and quarter.get('始値') is not None:
            yields.append(quarter['経常益利回り'])
            stock_prices_for_yield.append(quarter['始値'])
    
    # 3つのデータが揃っている場合のみ相関を計算し、最新の四半期のみに設定
    if len(yields) == 3 and len(stock_prices_for_yield) == 3:
        correlation_yield = np.corrcoef(yields, stock_prices_for_yield)[0, 1]
        # 最新の四半期データ（sorted_data[0]）にのみ相関値を設定
        sorted_data[0]['経常益利回り株価相関'] = round(correlation_yield, 3)


def get_fiscal_year_end_month(html: str) -> int:
    """通期データから決算月を取得（例：3月決算なら3を返す）"""
    soup = BeautifulSoup(html, 'lxml')
    
    # 複数のパターンで決算月を探す
    import re
    
    # パターン1: YYYY.MM形式（通期テーブルの標準形式）
    pattern1 = re.compile(r'(\d{4})\.(\d{2})')
    matches1 = pattern1.findall(str(soup))
    if matches1:
        # 複数年度のデータがある場合、最新年度の決算月を取得
        return int(matches1[-1][1])
    
    # パターン2: 連 YYYY.MM形式
    pattern2 = re.compile(r'連.*?(\d{4})\.(\d{2})')
    matches2 = pattern2.findall(str(soup))
    if matches2:
        return int(matches2[0][1])
    
    # パターン3: 単体 YYYY.MM形式
    pattern3 = re.compile(r'単体.*?(\d{4})\.(\d{2})')
    matches3 = pattern3.findall(str(soup))
    if matches3:
        return int(matches3[0][1])
    
    # パターン4: 四半期データから推定（全月対応）
    quarterly_pattern = re.compile(r'\d{2}\.\d{2}-(\d{2})')
    quarterly_matches = quarterly_pattern.findall(str(soup))
    if quarterly_matches:
        end_months = [int(m) for m in quarterly_matches if 1 <= int(m) <= 12]
        if end_months:
            # 最も頻繁に現れる決算月候補を返す
            from collections import Counter
            most_common = Counter(end_months).most_common(1)
            if most_common:
                return most_common[0][0]
    
    print("デバッグ: 決算月検出に失敗、HTMLの内容を確認します...")
    # デバッグ用：HTMLの一部を表示
    all_text = soup.get_text()
    relevant_lines = [line.strip() for line in all_text.split('\n') 
                     if (re.search(r'\d{4}\.\d{2}', line) or re.search(r'\d{2}\.\d{2}-\d{2}', line)) 
                     and line.strip()]
    for line in relevant_lines[:10]:  # 最初の10行を表示
        print(f"  {line}")
    
    # デフォルトは3月決算
    return 3


def determine_quarter(period: str, fiscal_year_end_month: int) -> str:
    """決算期から四半期（1Q〜4Q）を判定（決算月を基準に動的計算）"""
    # 期間から終了月を取得（例: "24.07-09" -> 9）
    if '-' in period:
        end_month = int(period.split('-')[1])
    else:
        return None
    
    # 決算月を基準に各四半期の終了月を計算
    # 4Q: 決算月
    # 3Q: 決算月-3 (1月未満の場合は+12して前年)
    # 2Q: 決算月+6 (13月以上の場合は-12して翌年)  
    # 1Q: 決算月+3 (13月以上の場合は-12して翌年)
    
    q4_end = fiscal_year_end_month
    q3_end = (fiscal_year_end_month - 3) if fiscal_year_end_month > 3 else (fiscal_year_end_month - 3 + 12)
    q2_end = (fiscal_year_end_month + 6) if fiscal_year_end_month <= 6 else (fiscal_year_end_month + 6 - 12)
    q1_end = (fiscal_year_end_month + 3) if fiscal_year_end_month <= 9 else (fiscal_year_end_month + 3 - 12)
    
    if end_month == q1_end:
        return "1Q"
    elif end_month == q2_end:
        return "2Q"
    elif end_month == q3_end:
        return "3Q"
    elif end_month == q4_end:
        return "4Q"
    
    return None


def extract_quarterly_data(html: str) -> List[Dict]:
    """四半期データを抽出"""
    soup = BeautifulSoup(html, 'lxml')
    quarterly_data = []
    
    # 全テーブルを検索してIで始まる四半期データ行を探す
    tables = soup.find_all('table')
    
    for table in tables:
        rows = table.find_all('tr')
        
        # このテーブルに四半期データがあるかチェック
        quarterly_rows = []
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if cells:
                first_cell = cells[0].get_text().strip()
                # 四半期データの識別：XX.XX-XX形式を含む行
                if re.search(r'\d{2}\.\d{2}-\d{2}', first_cell):
                    quarterly_rows.append(row)
        
        # デバッグ情報：このテーブルで見つかった四半期データ行数
        if len(quarterly_rows) > 0:
            print(f"テーブル内で四半期データを{len(quarterly_rows)}行発見")
            for i, row in enumerate(quarterly_rows[:3]):  # 最初の3行を表示
                cells = row.find_all(['td', 'th'])
                if cells:
                    first_cell_text = cells[0].get_text().strip()
                    print(f"  行{i+1}: {first_cell_text}")
        
        # 四半期データが8行以上あるテーブルを使用（最も完全なデータ）
        if len(quarterly_rows) >= 8:
            print(f"四半期データテーブルを発見: {len(quarterly_rows)}行")
            
            for row in quarterly_rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 8:  # 十分な列数があることを確認
                    data = {}
                    
                    # 決算期 (例: I   　 24.07-09)
                    period_text = cells[0].get_text().strip()
                    # 不要な文字を除去して期間部分だけ抽出
                    period_match = re.search(r'(\d{2}\.\d{2}-\d{2})', period_text)
                    if period_match:
                        data['決算期'] = period_match.group(1)
                    else:
                        continue
                    
                    # 売上高
                    data['売上高'] = parse_number(cells[1].get_text().strip())
                    
                    # 営業益（通常「－」なのでスキップ）
                    data['営業益'] = parse_number(cells[2].get_text().strip())
                    
                    # 経常益
                    data['経常益'] = parse_number(cells[3].get_text().strip())
                    
                    # 最終益
                    data['最終益'] = parse_number(cells[4].get_text().strip())
                    
                    # 修正1株益
                    data['修正1株益'] = parse_number(cells[5].get_text().strip())
                    
                    # 発表日とPDFリンク
                    announcement_cell = cells[7]
                    data['発表日'] = announcement_cell.get_text().strip()
                    
                    # 発表日セル内のリンクを探す
                    link = announcement_cell.find('a')
                    if link and link.get('href'):
                        href = link.get('href')
                        # 株探のPDFリンクを正しいURL形式に変換
                        # 例: /disclosures/pdf/20250807/140120250805531214/
                        # → https://tdnet-pdf.kabutan.jp/20250807/140120250805531214.pdf
                        if '/disclosures/pdf/' in href:
                            # パスから日付とファイルIDを抽出
                            match = re.search(r'/disclosures/pdf/(\d{8})/(\d+)/', href)
                            if match:
                                date_part = match.group(1)
                                file_id = match.group(2)
                                data['PDF_URL'] = f"https://tdnet-pdf.kabutan.jp/{date_part}/{file_id}.pdf"
                            else:
                                data['PDF_URL'] = f"https://kabutan.jp{href}" if href.startswith('/') else href
                        else:
                            data['PDF_URL'] = f"https://kabutan.jp{href}" if href.startswith('/') else href
                    else:
                        data['PDF_URL'] = None
                    
                    # PDFから資産合計と資本合計を取得（URLがある場合のみ）
                    data['資産合計'] = None
                    data['資本合計'] = None
                    
                    if data['PDF_URL']:
                        print(f"PDFから財政状態データを取得中: {data['決算期']}")
                        try:
                            pdf_content = download_pdf(data['PDF_URL'])
                            if pdf_content:
                                balance_data = extract_balance_sheet_data(pdf_content)
                                data['資産合計'] = balance_data.get('資産合計')
                                data['資本合計'] = balance_data.get('資本合計')
                                
                                if data['資産合計'] and data['資本合計']:
                                    print(f"  成功: 資産合計={data['資産合計']:,.0f}, 資本合計={data['資本合計']:,.0f}")
                                else:
                                    print(f"  警告: 財政状態データの一部が取得できませんでした")
                            else:
                                print(f"  エラー: PDFのダウンロードに失敗")
                        except Exception as e:
                            print(f"  エラー: PDF処理中に例外が発生 - {e}")
                    
                    quarterly_data.append(data)
            
            break  # 最初に見つかった完全なテーブルを使用
    
    # デバッグ: 四半期データが全く見つからない場合の詳細情報
    if not quarterly_data:
        print("\nデバッグ: 四半期データが見つからない詳細情報")
        print(f"全テーブル数: {len(tables)}")
        
        # 「I」で始まる行をすべて表示
        all_i_rows = []
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if cells:
                    first_cell = cells[0].get_text().strip()
                    if first_cell.startswith('I'):
                        all_i_rows.append(first_cell)
        
        print(f"「I」で始まる行の数: {len(all_i_rows)}")
        for i, row_text in enumerate(all_i_rows[:10]):  # 最初の10行を表示
            print(f"  I行{i+1}: {row_text}")
            # パターンマッチの確認
            has_length = len(row_text) > 8
            has_pattern = bool(re.search(r'\d{2}\.\d{2}-\d{2}', row_text))
            print(f"    長さ>8: {has_length}, パターンマッチ: {has_pattern}")
    
    # 決算期でソート（古い順）
    if quarterly_data:
        quarterly_data.sort(key=lambda x: x['決算期'])
    
    return quarterly_data


def parse_number(text: str) -> Optional[float]:
    """数値文字列をパース"""
    if not text or text == '-' or text == '－':
        return None
    
    # カンマを除去し、マイナス記号を正規化
    text = text.replace(',', '').replace('－', '-')
    
    try:
        return float(text)
    except ValueError:
        return None


def calculate_qoq_growth_rate(data: List[Dict], fiscal_year_end_month: int = 3) -> List[Dict]:
    """四半期成長率（経常益と売上高）と経常益利回りを計算して追加
    
    計算式: (現四半期 - 1年前の同四半期) / sum(abs(1期前), abs(2期前), abs(3期前), abs(4期前))
    """
    # 決算期でソート（古い順）
    sorted_data = sorted(data, key=lambda x: x['決算期'])
    
    # 各データに四半期を追加
    for item in sorted_data:
        item['四半期'] = determine_quarter(item['決算期'], fiscal_year_end_month)
        item['経常益利回り'] = None  # 初期化
    
    # 年度ごとの経常益累計を計算するためのディクショナリ
    fiscal_year_data = {}
    
    for i, current in enumerate(sorted_data):
        quarter = current.get('四半期')
        
        # 年度を判定（決算月に応じて動的に調整）
        year_str = current['決算期'][:2]
        base_year = int(year_str)
        
        # 期間の終了月を取得して年度を判定
        if '-' in current['決算期']:
            end_month = int(current['決算期'].split('-')[1])
        else:
            end_month = None
        
        # 決算月を基準に年度を判定
        if end_month:
            # 終了月が決算月より大きい場合は翌年度、以下の場合は同年度
            if end_month > fiscal_year_end_month:
                fiscal_year = base_year + 1
            else:
                fiscal_year = base_year
        else:
            fiscal_year = base_year
        
        # 年度データの初期化
        if fiscal_year not in fiscal_year_data:
            fiscal_year_data[fiscal_year] = {}
        
        # 四半期データを格納
        if quarter:
            fiscal_year_data[fiscal_year][quarter] = {
                'index': i,
                '経常益': current.get('経常益'),
                '資本合計': current.get('資本合計')
            }
    
    # 経常益利回りを計算
    for fiscal_year, quarters in fiscal_year_data.items():
        cumulative_ordinary_income = 0
        
        for q in ['1Q', '2Q', '3Q', '4Q']:
            if q in quarters and quarters[q]['経常益'] is not None:
                idx = quarters[q]['index']
                cumulative_ordinary_income += quarters[q]['経常益']
                capital = quarters[q]['資本合計']
                
                if capital and capital != 0:
                    if q == '1Q':
                        # 1Q: 経常益 * 4 / 資本合計
                        yield_rate = (quarters[q]['経常益'] * 4 / capital) * 100
                    elif q == '2Q':
                        # 2Q: 累計経常益 * 2 / 資本合計
                        yield_rate = (cumulative_ordinary_income * 2 / capital) * 100
                    elif q == '3Q':
                        # 3Q: 累計経常益 * 1.33 / 資本合計
                        yield_rate = (cumulative_ordinary_income * 1.33 / capital) * 100
                    elif q == '4Q':
                        # 4Q: 累計経常益 / 資本合計
                        yield_rate = (cumulative_ordinary_income / capital) * 100
                    
                    sorted_data[idx]['経常益利回り'] = round(yield_rate, 2)
                else:
                    sorted_data[idx]['経常益利回り'] = None
            elif q in quarters:
                idx = quarters[q]['index']
                sorted_data[idx]['経常益利回り'] = None
    
    # 成長率計算のために再度新しい順にソート
    sorted_data = sorted(sorted_data, key=lambda x: x['決算期'], reverse=True)
    
    for i, current in enumerate(sorted_data):
        # デフォルトで計算不可
        current['四半期成長率'] = None
        current['売上高成長率'] = None
        current['四半期割安率_四半期平均'] = None
        current['四半期割安率_前年同期ベース'] = None
        current['四半期割安率_前四半期'] = None
        
        # 経常益の成長率計算
        if current['経常益'] is not None:
            # 1年前（4期前）のデータが必要
            if i + 4 < len(sorted_data):
                year_ago = sorted_data[i + 4]
                
                # 1年前の経常益がない場合はスキップ
                if year_ago['経常益'] is not None:
                    # 直近4期分のデータ（現四半期、1期前、2期前、3期前）を取得
                    recent_quarters = [current['経常益']]  # 現四半期を含める
                    for j in range(1, 4):  # 1期前〜3期前
                        if i + j < len(sorted_data) and sorted_data[i + j]['経常益'] is not None:
                            recent_quarters.append(sorted_data[i + j]['経常益'])
                        else:
                            break
                    
                    # 4期分のデータが揃っている場合
                    if len(recent_quarters) == 4:
                        # 分母を計算（現四半期＋直近3期の絶対値の合計）
                        denominator = sum(abs(value) for value in recent_quarters)
                        
                        # 分母が0でない場合
                        if denominator != 0:
                            # 成長率を計算（パーセント表記）
                            numerator = current['経常益'] - year_ago['経常益']
                            growth_rate = (numerator / denominator) * 100
                            current['四半期成長率'] = round(growth_rate, 2)
        
        # 売上高の成長率計算
        if current['売上高'] is not None:
            # 1年前（4期前）のデータが必要
            if i + 4 < len(sorted_data):
                year_ago = sorted_data[i + 4]
                
                # 1年前の売上高がない場合はスキップ
                if year_ago['売上高'] is not None:
                    # 直近4期分のデータ（現四半期、1期前、2期前、3期前）を取得
                    recent_quarters_sales = [current['売上高']]  # 現四半期を含める
                    for j in range(1, 4):  # 1期前〜3期前
                        if i + j < len(sorted_data) and sorted_data[i + j]['売上高'] is not None:
                            recent_quarters_sales.append(sorted_data[i + j]['売上高'])
                        else:
                            break
                    
                    # 4期分のデータが揃っている場合
                    if len(recent_quarters_sales) == 4:
                        # 分母を計算（現四半期＋直近3期の絶対値の合計）
                        denominator_sales = sum(abs(value) for value in recent_quarters_sales)
                        
                        # 分母が0でない場合
                        if denominator_sales != 0:
                            # 成長率を計算（パーセント表記）
                            numerator_sales = current['売上高'] - year_ago['売上高']
                            growth_rate_sales = (numerator_sales / denominator_sales) * 100
                            current['売上高成長率'] = round(growth_rate_sales, 2)
        
        # 四半期割安率_四半期平均の計算（経常益利回りのデータを使用）
        if current['経常益利回り'] is not None:
            # 1年前（4期前）のデータが必要
            if i + 4 < len(sorted_data):
                year_ago = sorted_data[i + 4]
                
                # 1年前の経常益利回りがない場合はスキップ
                if year_ago['経常益利回り'] is not None:
                    # 直近4期分のデータ（現四半期、1期前、2期前、3期前）を取得
                    recent_quarters_yield = [current['経常益利回り']]  # 現四半期を含める
                    for j in range(1, 4):  # 1期前〜3期前
                        if i + j < len(sorted_data) and sorted_data[i + j]['経常益利回り'] is not None:
                            recent_quarters_yield.append(sorted_data[i + j]['経常益利回り'])
                        else:
                            break
                    
                    # 4期分のデータが揃っている場合
                    if len(recent_quarters_yield) == 4:
                        # 分母を計算（現四半期＋直近3期の絶対値の合計）
                        denominator_yield = sum(abs(value) for value in recent_quarters_yield)
                        
                        # 分母が0でない場合
                        if denominator_yield != 0:
                            # 四半期割安率を計算（パーセント表記）
                            numerator_yield = current['経常益利回り'] - year_ago['経常益利回り']
                            discount_rate = (numerator_yield / denominator_yield) * 100
                            current['四半期割安率_四半期平均'] = round(discount_rate, 2)
        
        # 四半期割安率_前年同期ベースの計算（経常益利回りのデータを使用）
        if current['経常益利回り'] is not None:
            # 前年同期（5期前）のデータが必要
            if i + 4 < len(sorted_data):
                same_quarter_last_year = sorted_data[i + 4]
                
                # 前年同期の経常益利回りがある場合
                if same_quarter_last_year['経常益利回り'] is not None:
                    # 単純な差分計算
                    current['四半期割安率_前年同期ベース'] = round(
                        current['経常益利回り'] - same_quarter_last_year['経常益利回り'], 2
                    )
        
        # 四半期割安率_前四半期の計算（経常益利回りのデータを使用）
        if current['経常益利回り'] is not None:
            # 前四半期（1期前）のデータが必要
            if i + 1 < len(sorted_data):
                previous_quarter = sorted_data[i + 1]
                
                # 前四半期の経常益利回りがある場合
                if previous_quarter['経常益利回り'] is not None:
                    # 単純な差分計算
                    current['四半期割安率_前四半期'] = round(
                        current['経常益利回り'] - previous_quarter['経常益利回り'], 2
                    )
    
    return data


def save_to_csv(data: List[Dict], code: str = "9984", fiscal_year_end_month: int = 3):
    """データをCSVファイルに保存"""
    if not data:
        print("保存するデータがありません")
        return None
    
    # 四半期成長率と経常益利回りを計算
    data_with_growth = calculate_qoq_growth_rate(data, fiscal_year_end_month)
    
    # 週足データを取得
    print(f"\n週足データを取得中...")
    weekly_data = fetch_weekly_stock_data(code)
    
    # 各四半期データに株価情報を追加
    for item in data_with_growth:
        if item.get('発表日'):
            stock_info = find_stock_price_after_announcement(item['発表日'], weekly_data)
            item['株価日付'] = stock_info['株価日付']
            item['始値'] = stock_info['始値']
        else:
            item['株価日付'] = None
            item['始値'] = None
    
    # 株価相関を計算
    calculate_stock_correlations(data_with_growth)
    
    df = pd.DataFrame(data_with_growth)
    
    # 不要な列を削除
    columns_to_drop = ['営業益', '最終益', '修正1株益']
    df = df.drop(columns=columns_to_drop, errors='ignore')
    
    # 列の順番を指定（相関列を最後に追加）
    column_order = ['決算期', '四半期', '売上高', '経常益', '発表日', 'PDF_URL', '資産合計', '資本合計', '売上高成長率', '四半期成長率', '経常益利回り', '四半期割安率_四半期平均', '四半期割安率_前年同期ベース', '四半期割安率_前四半期', '株価日付', '始値', '四半期成長率株価相関', '経常益利回り株価相関']
    df = df[column_order]
    
    # 数値列の型を設定
    numeric_columns = ['売上高', '経常益', '資産合計', '資本合計', '売上高成長率', '四半期成長率', '経常益利回り', '四半期割安率_四半期平均', '四半期割安率_前年同期ベース', '四半期割安率_前四半期', '始値', '四半期成長率株価相関', '経常益利回り株価相関']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # CSVに保存
    filename = f"quarterly_data_{code}.csv"
    output_path = f"data/output/{filename}"
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"\nデータを {output_path} に保存しました")
    print(f"取得データ数: {len(df)}件")
    print(f"株価データマッチ数: {df['始値'].notna().sum()}件")
    
    # 相関値の出力
    if df['四半期成長率株価相関'].notna().any():
        corr_growth = df['四半期成長率株価相関'].iloc[0]
        print(f"四半期成長率と株価の相関: {corr_growth:.3f}")
    
    if df['経常益利回り株価相関'].notna().any():
        corr_yield = df['経常益利回り株価相関'].iloc[0]
        print(f"経常益利回りと株価の相関: {corr_yield:.3f}")
    
    # データプレビュー
    print("\n=== 取得データプレビュー ===")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.float_format', lambda x: '%.2f' % x if pd.notna(x) else '')
    print(df.to_string(index=False))
    
    # 成長率の検証出力（最新データ）
    latest_data = df.iloc[-1]
    if pd.notna(latest_data.get('四半期成長率')):
        print(f"\n最新四半期（{latest_data['決算期']}）の成長率: {latest_data['四半期成長率']:.2f}%")
    if pd.notna(latest_data.get('始値')):
        print(f"最新四半期の発表日翌日株価: {latest_data['始値']:.0f}円 ({latest_data['株価日付']})")
    
    return df


def main():
    """メイン処理"""
    import sys
    code = sys.argv[1] if len(sys.argv) > 1 else "3799"
    print(f"株探から四半期データを取得します...")
    print(f"対象: {code}")
    
    # HTMLを取得
    html = fetch_kabutan_page(code)
    if not html:
        print("エラー: ページの取得に失敗しました")
        return
    
    # 決算月を取得
    fiscal_year_end_month = get_fiscal_year_end_month(html)
    print(f"決算月: {fiscal_year_end_month}月")
    
    # 四半期データを抽出
    quarterly_data = extract_quarterly_data(html)
    
    if not quarterly_data:
        print("エラー: 四半期データが見つかりませんでした")
        return
    
    # CSVに保存
    df = save_to_csv(quarterly_data, code, fiscal_year_end_month)
    
    if df is not None:
        print(f"\n処理が完了しました！")
        print(f"期間範囲: {df['決算期'].min()} ～ {df['決算期'].max()}")
    else:
        print("エラー: データの保存に失敗しました")


if __name__ == "__main__":
    main()