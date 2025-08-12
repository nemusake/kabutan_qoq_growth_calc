#!/usr/bin/env python3
"""PDF決算資料から財政状態データを抽出"""

import requests
import pdfplumber
import re
from io import BytesIO
from typing import Optional, Tuple, Dict
import time


def download_pdf(url: str) -> Optional[BytesIO]:
    """PDFをダウンロード"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/pdf,*/*',
            'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
            'Referer': 'https://kabutan.jp/'
        }
        
        print(f"PDFをダウンロード中: {url}")
        
        # セッションを使用してクッキーを保持
        session = requests.Session()
        session.headers.update(headers)
        
        # まずページにアクセスしてリダイレクトを確認
        response = session.get(url, allow_redirects=True)
        response.raise_for_status()
        
        print(f"Content-Type: {response.headers.get('content-type', 'Unknown')}")
        print(f"Content-Length: {len(response.content)} bytes")
        
        # PDFかどうかチェック
        content_type = response.headers.get('content-type', '').lower()
        if 'pdf' not in content_type and len(response.content) < 1000:
            print("PDFではないようです。HTMLページかもしれません。")
            print(f"レスポンス内容（最初の500文字）: {response.text[:500]}")
            return None
        
        # 少し待機してサーバーに負荷をかけないようにする
        time.sleep(1)
        
        return BytesIO(response.content)
    except Exception as e:
        print(f"PDFのダウンロードに失敗: {e}")
        return None


def extract_balance_sheet_data(pdf_content: BytesIO) -> Dict[str, Optional[float]]:
    """PDFから資産合計と資本合計を抽出"""
    result = {
        '資産合計': None,
        '資本合計': None
    }
    
    try:
        with pdfplumber.open(pdf_content) as pdf:
            # 最初の3ページをチェック（通常1ページ目にある）
            for page_num in range(min(3, len(pdf.pages))):
                page = pdf.pages[page_num]
                text = page.extract_text()
                
                if text:
                    print(f"Page {page_num + 1} テキストを解析中...")
                    
                    # デバッグ用：最初の1000文字を表示
                    if page_num == 0:
                        print(f"1ページ目の内容（最初の1000文字）:\n{text[:1000]}")
                    
                    # 財政状態、貸借対照表関連のキーワードをチェック
                    if ('財政状態' in text or '貸借対照表' in text or 
                        '資産合計' in text or '資本合計' in text or
                        '総資産' in text or '純資産' in text):
                        
                        # 「連結財政状態」部分からデータを抽出
                        # 行単位で分析
                        lines = text.split('\n')
                        
                        # 財政状態セクションを見つけて数値を抽出
                        in_financial_position = False
                        
                        for i, line in enumerate(lines):
                            line = line.strip()
                            
                            # 財政状態セクションの開始を検出
                            if '連結財政状態' in line or ('資産合計' in line and '資本合計' in line):
                                in_financial_position = True
                                print(f"財政状態セクション開始: {line}")
                                continue
                            
                            # 財政状態セクション内で最新四半期のデータ行を探す
                            if in_financial_position:
                                # パターン1: 年度を含む行（例：2026年３月期第１四半期）
                                # パターン2: テーブルのデータ行（数値のみが複数並ぶ行）
                                # 全角・半角カンマ両方に対応
                                large_numbers = re.findall(r'[\d,，]{6,}', line)  # 全角カンマも対応
                                print(f"財政状態セクション内の行: {repr(line.strip())}")  # 文字の詳細確認
                                print(f"  抽出された数値: {large_numbers}")
                                
                                # 数値パターンをさらに詳しくテスト
                                test_patterns = [
                                    r'[\d,]{6,}',     # 半角カンマ
                                    r'[\d,，]{6,}',   # 全角・半角カンマ
                                    r'\d{6,}',        # カンマなし6桁以上
                                    r'[\d,，\s]{6,}'  # スペースも含む
                                ]
                                for i, pattern in enumerate(test_patterns):
                                    test_result = re.findall(pattern, line)
                                    if test_result:
                                        print(f"    パターン{i+1} ({pattern}): {test_result}")
                                
                                if ((('年' in line or '四半期' in line or '期' in line) and large_numbers) or
                                    (len(large_numbers) >= 2)):
                                    
                                    print(f"財政状態データ行: {line}")
                                    
                                    # 大きな数値を順番に抽出（通常、資産合計が最初、資本合計が2番目）
                                    numbers = large_numbers  # 既に上で取得済み
                                    print(f"抽出された大きな数値: {numbers}")
                                    
                                    if len(numbers) >= 2:
                                        # 最初の数値を資産合計として試す
                                        print(f"パース前の数値1: {repr(numbers[0])}")
                                        asset_candidate = parse_balance_number(numbers[0])
                                        print(f"パース後の資産合計候補: {asset_candidate}")
                                        
                                        # 2番目の数値を資本合計として試す  
                                        print(f"パース前の数値2: {repr(numbers[1])}")
                                        equity_candidate = parse_balance_number(numbers[1])
                                        print(f"パース後の資本合計候補: {equity_candidate}")
                                        
                                        # 妥当性チェック（資産合計 > 資本合計）
                                        print(f"妥当性チェック: asset={asset_candidate}, equity={equity_candidate}")
                                        print(f"  条件1 (両方存在): {bool(asset_candidate and equity_candidate)}")
                                        print(f"  条件2 (asset > equity): {asset_candidate > equity_candidate if (asset_candidate and equity_candidate) else 'N/A'}")
                                        print(f"  条件3 (asset > 100M): {asset_candidate > 100 if asset_candidate else 'N/A'}")
                                        print(f"  条件4 (equity > 50M): {equity_candidate > 50 if equity_candidate else 'N/A'}")
                                        
                                        if (asset_candidate and equity_candidate and 
                                            asset_candidate > equity_candidate and
                                            asset_candidate > 100 and   # 1億円以上（百万円単位）
                                            equity_candidate > 50):      # 5000万円以上（百万円単位）
                                            
                                            result['資産合計'] = asset_candidate
                                            result['資本合計'] = equity_candidate
                                            print(f"資産合計を発見: {asset_candidate:,.0f}")
                                            print(f"資本合計を発見: {equity_candidate:,.0f}")
                                            break
                                    
                                # セクション終了の判定（次のセクションの開始）
                                elif ('配当' in line or '株式' in line or line == ''):
                                    in_financial_position = False
                            
                            # 既に両方見つかったら終了
                            if result['資産合計'] and result['資本合計']:
                                break
                            
                            # 個別に探す場合のパターン
                            if not result['資産合計'] and ('資産合計' in line or '総資産' in line):
                                numbers = re.findall(r'[\d,]+', line)
                                for num in numbers:
                                    value = parse_balance_number(num)
                                    if value and value > 100:  # 1億円以上（百万円単位）
                                        result['資産合計'] = value
                                        print(f"資産合計を発見: {value:,.0f}")
                                        break
                            
                            if not result['資本合計'] and ('資本合計' in line or '純資産' in line):
                                numbers = re.findall(r'[\d,]+', line)
                                for num in numbers:
                                    value = parse_balance_number(num)
                                    if value and value > 50:  # 5000万円以上（百万円単位）
                                        result['資本合計'] = value
                                        print(f"資本合計を発見: {value:,.0f}")
                                        break
                        
                        # 従来のパターンマッチングもバックアップとして実行
                        if not result['資産合計'] or not result['資本合計']:
                            asset_patterns = [
                                r'資産合計[^\d]*?([\d,]+)',
                                r'総資産[^\d]*?([\d,]+)'
                            ]
                            
                            equity_patterns = [
                                r'資本合計[^\d]*?([\d,]+)',
                                r'純資産合計[^\d]*?([\d,]+)',
                                r'純資産[^\d]*?([\d,]+)'
                            ]
                            
                            for pattern in asset_patterns:
                                if result['資産合計']:
                                    break
                                matches = re.findall(pattern, text)
                                for match in matches:
                                    value = parse_balance_number(match)
                                    if value and value > 100:  # 1億円以上（百万円単位）
                                        result['資産合計'] = value
                                        print(f"パターンマッチで資産合計を発見: {value:,.0f}")
                                        break
                            
                            for pattern in equity_patterns:
                                if result['資本合計']:
                                    break
                                matches = re.findall(pattern, text)
                                for match in matches:
                                    value = parse_balance_number(match)
                                    if value and value > 50:  # 5000万円以上（百万円単位）
                                        result['資本合計'] = value
                                        print(f"パターンマッチで資本合計を発見: {value:,.0f}")
                                        break
                        
                        # 両方見つかったら終了
                        if result['資産合計'] and result['資本合計']:
                            break
                
                # テーブルからも探してみる
                try:
                    tables = page.extract_tables()
                    for table in tables:
                        for row in table:
                            if row and len(row) >= 2:
                                # 最初の列に項目名、2列目以降に数値があることを期待
                                first_col = str(row[0]).strip() if row[0] else ""
                                
                                if '資産合計' in first_col or '総資産' in first_col:
                                    for col in row[1:]:
                                        if col:
                                            value = parse_balance_number(str(col))
                                            if value and value > 100:  # 1億円以上（百万円単位）
                                                result['資産合計'] = value
                                                print(f"テーブルから資産合計を発見: {value:,.0f}")
                                                break
                                
                                if ('資本合計' in first_col or '純資産合計' in first_col or 
                                    '株主資本合計' in first_col):
                                    for col in row[1:]:
                                        if col:
                                            value = parse_balance_number(str(col))
                                            if value and value > 50:  # 5000万円以上（百万円単位）
                                                result['資本合計'] = value
                                                print(f"テーブルから資本合計を発見: {value:,.0f}")
                                                break
                except Exception as e:
                    print(f"テーブル解析でエラー: {e}")
                    continue
                
                # 両方見つかったら終了
                if result['資産合計'] and result['資本合計']:
                    break
    
    except Exception as e:
        print(f"PDF解析エラー: {e}")
    
    return result


def parse_balance_number(text: str) -> Optional[float]:
    """財政状態の数値をパース（百万円単位）"""
    if not text:
        return None
    
    # カンマを除去し、数値以外の文字を除去
    cleaned = re.sub(r'[^\d,.-]', '', str(text))
    cleaned = cleaned.replace(',', '')
    
    try:
        value = float(cleaned)
        # 負の値は無視（資産合計や資本合計に負の値はない）
        if value > 0:
            return value
    except (ValueError, TypeError):
        pass
    
    return None


def test_pdf_extraction(pdf_url: str):
    """PDFからのデータ抽出をテスト"""
    print(f"テスト開始: {pdf_url}")
    
    pdf_content = download_pdf(pdf_url)
    if not pdf_content:
        print("PDFのダウンロードに失敗しました")
        return
    
    balance_data = extract_balance_sheet_data(pdf_content)
    
    print("\n=== 抽出結果 ===")
    for key, value in balance_data.items():
        if value:
            print(f"{key}: {value:,.0f} 百万円")
        else:
            print(f"{key}: 取得できませんでした")


if __name__ == "__main__":
    # 問題のある24.10-12のPDFをテスト
    test_url = "https://tdnet-pdf.kabutan.jp/20250212/140120250210568594.pdf"
    test_pdf_extraction(test_url)