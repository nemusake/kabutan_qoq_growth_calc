#!/usr/bin/env python3
"""codelist.csvの複数銘柄を一括処理し、最新データをまとめてCSV出力するバッチツール"""

import pandas as pd
import os
import sys
import time
from typing import List, Dict, Optional
from qq import (
    fetch_kabutan_page, 
    get_fiscal_year_end_month, 
    extract_quarterly_data, 
    calculate_qoq_growth_rate,
    fetch_weekly_stock_data,
    find_stock_price_after_announcement,
    calculate_stock_correlations
)


def load_code_list(csv_file: str = "codelist.csv") -> List[Dict[str, str]]:
    """codelist.csvから証券コードと銘柄名のリストを読み込み"""
    try:
        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        # NaNや空の値を除外
        df = df.dropna(subset=['コード'])
        
        # 証券コードと銘柄名の組み合わせを辞書のリストで返す
        code_list = []
        for _, row in df.iterrows():
            code = str(row['コード']).strip()
            name = str(row['銘柄名']).strip() if pd.notna(row['銘柄名']) else ""
            if code:
                code_list.append({'code': code, 'name': name})
        
        print(f"codelist.csvから{len(code_list)}件の証券コードを読み込みました")
        return code_list
    except FileNotFoundError:
        print(f"エラー: {csv_file}ファイルが見つかりません")
        return []
    except Exception as e:
        print(f"codelist.csv読み込みエラー: {e}")
        return []


def process_single_stock(code: str, name: str = "") -> Optional[Dict]:
    """単一銘柄の最新データを取得"""
    print(f"\n=== {code} ({name}) の処理開始 ===")
    
    try:
        # HTMLを取得
        html = fetch_kabutan_page(code)
        if not html:
            print(f"{code}: ページの取得に失敗")
            return None
        
        # 決算月を取得
        fiscal_year_end_month = get_fiscal_year_end_month(html)
        print(f"{code}: 決算月 = {fiscal_year_end_month}月")
        
        # 四半期データを抽出
        quarterly_data = extract_quarterly_data(html)
        if not quarterly_data:
            print(f"{code}: 四半期データが見つかりません")
            return None
        
        # 成長率計算
        data_with_growth = calculate_qoq_growth_rate(quarterly_data, fiscal_year_end_month)
        
        # 週足データを取得
        print(f"{code}: 週足データを取得中...")
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
        
        # 最新データ（新しい順にソート後の最初のデータ）を取得
        sorted_data = sorted(data_with_growth, key=lambda x: x['決算期'], reverse=True)
        latest_data = sorted_data[0]
        
        # 必要な項目のみ抽出（パーセント項目は100分の1に変換）
        # 列順: コード、銘柄名、株価日付、始値、発表日、決算期...
        result = {
            'コード': code,
            '銘柄名': name,
            '株価日付': latest_data.get('株価日付'),
            '始値': latest_data.get('始値'),
            '発表日': latest_data.get('発表日'),
            '決算期': latest_data.get('決算期'),
            '四半期': latest_data.get('四半期'),
            '売上高': latest_data.get('売上高'),
            '経常益': latest_data.get('経常益'),
            '資本合計(純資産)': latest_data.get('資本合計'),
            '売上高成長率': latest_data.get('売上高成長率') / 100 if latest_data.get('売上高成長率') is not None else None,
            '四半期成長率': latest_data.get('四半期成長率') / 100 if latest_data.get('四半期成長率') is not None else None,
            '経常益利回り': latest_data.get('経常益利回り') / 100 if latest_data.get('経常益利回り') is not None else None,
            '四半期割安率_四半期平均': latest_data.get('四半期割安率_四半期平均') / 100 if latest_data.get('四半期割安率_四半期平均') is not None else None,
            '四半期割安率_前年同期ベース': latest_data.get('四半期割安率_前年同期ベース') / 100 if latest_data.get('四半期割安率_前年同期ベース') is not None else None,
            '四半期割安率_前四半期': latest_data.get('四半期割安率_前四半期') / 100 if latest_data.get('四半期割安率_前四半期') is not None else None,
            '四半期成長率株価相関': latest_data.get('四半期成長率株価相関'),
            '経常益利回り株価相関': latest_data.get('経常益利回り株価相関')
        }
        
        print(f"{code}: 最新データ取得完了 ({latest_data.get('決算期')} {latest_data.get('四半期')})")
        if latest_data.get('始値'):
            print(f"{code}: 株価データマッチ成功 ({latest_data.get('株価日付')} - {latest_data.get('始値')}円)")
        return result
        
    except Exception as e:
        print(f"{code}: 処理中にエラーが発生: {e}")
        return None


def create_batch_summary(results: List[Dict], output_file: str = "data/output/batch_summary.csv"):
    """バッチ処理結果をCSVファイルに保存"""
    if not results:
        print("保存するデータがありません")
        return None
    
    # データフレームを作成
    df = pd.DataFrame(results)
    
    # 数値列の型を設定
    numeric_columns = [
        '売上高', '経常益', '資本合計(純資産)', '売上高成長率', '四半期成長率', 
        '経常益利回り', '四半期割安率_四半期平均', '四半期割安率_前年同期ベース', '四半期割安率_前四半期',
        '始値', '四半期成長率株価相関', '経常益利回り株価相関'
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 出力ディレクトリを作成
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # CSVに保存
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"\n=== バッチ処理結果 ===")
    print(f"処理完了銘柄数: {len(df)}件")
    print(f"保存先: {output_file}")
    
    # サマリー表示
    print("\n=== データプレビュー ===")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.float_format', lambda x: '%.2f' % x if pd.notna(x) else '')
    print(df.to_string(index=False))
    
    return df


def main():
    """メイン処理"""
    print("株探バッチ処理ツール開始")
    print("codelist.csvの銘柄を一括処理し、最新データをまとめてCSV出力します")
    
    # 証券コードリストを読み込み
    code_list = load_code_list()
    if not code_list:
        print("処理する証券コードがありません")
        return
    
    print(f"処理対象: {code_list}")
    
    # 各銘柄を処理
    results = []
    total_codes = len(code_list)
    
    for i, stock_info in enumerate(code_list, 1):
        code = stock_info['code']
        name = stock_info['name']
        print(f"\n[{i}/{total_codes}] 処理中...")
        
        result = process_single_stock(code, name)
        if result:
            results.append(result)
        
        # サーバー負荷軽減のため待機（最後の銘柄以外）
        if i < total_codes:
            print("次の銘柄処理まで3秒待機...")
            time.sleep(3)
    
    # 結果をCSVに保存
    if results:
        create_batch_summary(results)
        print(f"\n処理完了！ {len(results)}/{total_codes} 銘柄のデータを取得しました")
    else:
        print("\n処理できた銘柄がありませんでした")


if __name__ == "__main__":
    main()