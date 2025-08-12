# QoQ データ実装詳細

株探四半期成長率計算ツールの技術実装について詳細に説明します。

## アーキテクチャ概要

### モジュール構成

```
src/
├── qoq_data.py          # メインプログラム
├── pdf_analyzer.py      # PDF解析専用モジュール
└── __init__.py          # パッケージ初期化
```

### 新機能: 株価データ取得・相関計算

- 週足データの複数ページ取得
- 発表日+1日ロジックでの株価マッチング
- 四半期成長率と株価の相関計算
- 経常益利回りと株価の相関計算

### 依存関係

```toml
dependencies = [
    "pandas>=2.0.0",        # データ処理・CSV出力
    "requests>=2.31.0",     # HTTP通信
    "beautifulsoup4>=4.12.0", # HTML解析
    "lxml>=4.9.0",          # HTML/XMLパーサー
    "PyPDF2>=3.0.0",        # PDF処理（サブ）
    "pdfplumber>=0.10.0",   # PDF解析（メイン）
]
```

## 実装詳細

### 1. メインプログラム (qoq_data.py)

#### 1.1 HTMLデータ取得

```python
def fetch_kabutan_page(code: str = "9984") -> Optional[str]:
    url = f"https://kabutan.jp/stock/finance?code={code}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36...'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"ページの取得に失敗しました: {e}")
        return None
```

**実装ポイント**:
- User-Agent設定でブラウザアクセスを模擬
- 例外処理でネットワークエラーをキャッチ
- オプションで証券コード変更可能

#### 1.2 四半期データ抽出

```python
def extract_quarterly_data(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, 'lxml')
    quarterly_data = []
    
    # 全テーブルを検索
    tables = soup.find_all('table')
    
    for table in tables:
        rows = table.find_all('tr')
        quarterly_rows = []
        
        # 四半期データ行を特定
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if cells:
                first_cell = cells[0].get_text().strip()
                if (first_cell.startswith('I') and 
                    len(first_cell) > 8 and 
                    ('23.' in first_cell or '24.' in first_cell or '25.' in first_cell) and
                    '-' in first_cell):
                    quarterly_rows.append(row)
        
        # 8行以上のデータを持つテーブルを採用
        if len(quarterly_rows) >= 8:
            # データ抽出処理
            break
```

**実装ポイント**:
- lxmlパーサーで高速HTML解析
- 複数テーブルから最適なものを自動選択
- 四半期データの識別条件を複数設定

#### 1.3 データ構造設計

```python
@dataclass
class QuarterlyData:
    period: str                      # 決算期
    revenue: Optional[float]         # 売上高
    ordinary_income: Optional[float] # 経常益
    net_income: Optional[float]      # 最終益
    eps_adjusted: Optional[float]    # 修正1株益
    announcement_date: str           # 発表日
    pdf_url: Optional[str]          # PDF URL
    total_assets: Optional[float]    # 資産合計
    total_equity: Optional[float]    # 資本合計
```

**設計思想**:
- Optional型で欠損データに対応
- 型安全性を確保
- 将来の拡張性を考慮

#### 1.4 PDFリンク変換

```python
# 株探内部リンク → 実際のPDFリンク変換
if '/disclosures/pdf/' in href:
    match = re.search(r'/disclosures/pdf/(\d{8})/(\d+)/', href)
    if match:
        date_part = match.group(1)  # 20250807
        file_id = match.group(2)    # 140120250805531214
        pdf_url = f"https://tdnet-pdf.kabutan.jp/{date_part}/{file_id}.pdf"
```

**実装ポイント**:
- 正規表現で日付とファイルIDを抽出
- 統一されたPDFホストドメインに変換
- エラー時はフォールバック処理

### 2. PDF解析モジュール (pdf_analyzer.py)

#### 2.1 PDF取得処理

```python
def download_pdf(url: str) -> Optional[BytesIO]:
    headers = {
        'User-Agent': 'Mozilla/5.0...',
        'Accept': 'application/pdf,*/*',
        'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
        'Referer': 'https://kabutan.jp/'
    }
    
    session = requests.Session()
    session.headers.update(headers)
    
    response = session.get(url, allow_redirects=True)
    
    # Content-Type確認
    content_type = response.headers.get('content-type', '').lower()
    if 'pdf' not in content_type and len(response.content) < 1000:
        return None
    
    time.sleep(1)  # サーバー負荷軽減
    return BytesIO(response.content)
```

**実装ポイント**:
- セッション管理でクッキー保持
- 適切なHTTPヘッダー設定
- Content-Type検証
- レート制限（1秒間隔）

#### 2.2 財政状態データ抽出

```python
def extract_balance_sheet_data(pdf_content: BytesIO) -> Dict[str, Optional[float]]:
    result = {'資産合計': None, '資本合計': None}
    
    with pdfplumber.open(pdf_content) as pdf:
        # 最初の3ページをチェック
        for page_num in range(min(3, len(pdf.pages))):
            page = pdf.pages[page_num]
            text = page.extract_text()
            
            if text:
                lines = text.split('\n')
                in_financial_position = False
                
                for line in lines:
                    line = line.strip()
                    
                    # 財政状態セクション検出
                    if '連結財政状態' in line or ('資産合計' in line and '資本合計' in line):
                        in_financial_position = True
                        continue
                    
                    # データ行検出・抽出
                    if in_financial_position:
                        if (('年' in line or '四半期' in line) and 
                            re.search(r'[\d,]{8,}', line)):
                            
                            numbers = re.findall(r'[\d,]{8,}', line)
                            
                            if len(numbers) >= 2:
                                asset_candidate = parse_balance_number(numbers[0])
                                equity_candidate = parse_balance_number(numbers[1])
                                
                                # 妥当性チェック
                                if (asset_candidate and equity_candidate and 
                                    asset_candidate > equity_candidate and
                                    asset_candidate > 10000000 and
                                    equity_candidate > 1000000):
                                    
                                    result['資産合計'] = asset_candidate
                                    result['資本合計'] = equity_candidate
                                    break
```

**実装ポイント**:
- pdfplumberで高精度テキスト抽出
- 状態管理で関連セクションを追跡
- 複数の検証条件で精度向上
- 大きな数値パターンで財務データを特定

#### 2.3 数値解析処理

```python
def parse_balance_number(text: str) -> Optional[float]:
    if not text or text == '-' or text == '－':
        return None
    
    # カンマ除去、マイナス記号正規化
    cleaned = re.sub(r'[^\d,.-]', '', str(text))
    cleaned = cleaned.replace(',', '')
    
    try:
        value = float(cleaned)
        # 負の値は無視（資産・資本に負値はない）
        if value > 0:
            return value
    except (ValueError, TypeError):
        pass
    
    return None
```

**実装ポイント**:
- 文字列クリーニング処理
- 型変換エラーハンドリング
- ビジネスロジック検証（正値のみ）

### 3. データ処理・出力

#### 3.1 決算月自動取得

```python
def get_fiscal_year_end_month(html: str) -> int:
    """通期データから決算月を取得（例：3月決算なら3を返す）"""
    soup = BeautifulSoup(html, 'lxml')
    
    # 通期データのテーブルから決算期を探す
    # 例: 連 YYYY.MMの形式
    pattern = re.compile(r'連.*?(\d{4})\.(\d{2})')
    
    matches = pattern.findall(str(soup))
    if matches:
        # 最初に見つかった決算月を返す
        return int(matches[0][1])
    
    # デフォルトは3月決算
    return 3
```

**実装ポイント**:
- 正規表現で通期データから決算月を自動検出
- 複数の銘柄に対応（3月、12月、8月決算など）
- フォールバック機能でデフォルト3月決算

#### 3.2 四半期自動判定

```python
def determine_quarter(period: str, fiscal_year_end_month: int) -> str:
    """決算期から四半期（1Q〜4Q）を判定（決算月を基準に動的計算）"""
    # 期間から終了月を取得（例: "24.07-09" -> 9）
    if '-' in period:
        end_month = int(period.split('-')[1])
    else:
        return None
    
    # 決算月を基準に各四半期の終了月を計算
    # 4Q: 決算月
    # 3Q: 決算月-3 (ただし1月未満の場合は+9)
    # 2Q: 決算月-6 (ただし1月未満の場合は+6)  
    # 1Q: 決算月-9 (ただし1月未満の場合は+3)
    
    q4_end = fiscal_year_end_month
    q3_end = (fiscal_year_end_month - 3) if fiscal_year_end_month > 3 else (fiscal_year_end_month + 9)
    q2_end = (fiscal_year_end_month - 6) if fiscal_year_end_month > 6 else (fiscal_year_end_month + 6)
    q1_end = (fiscal_year_end_month - 9) if fiscal_year_end_month > 9 else (fiscal_year_end_month + 3)
    
    if end_month == q1_end:
        return "1Q"
    elif end_month == q2_end:
        return "2Q"
    elif end_month == q3_end:
        return "3Q"
    elif end_month == q4_end:
        return "4Q"
    
    return None
```

**実装ポイント**:
- 決算月を基準にした動的四半期計算
- 任意の決算月に対応（例：8月決算なら06-08月が4Q）
- モジュール設計で再利用可能

#### 3.3 四半期成長率計算

```python
def calculate_qoq_growth_rate(data: List[Dict], fiscal_year_end_month: int = 3) -> List[Dict]:
    """四半期成長率（経常益と売上高）と経常益利回りを計算して追加"""
    
    # 成長率計算: (現四半期 - 1年前の同四半期) / sum(abs(現四半期), abs(1期前), abs(2期前), abs(3期前))
    for i, current in enumerate(sorted_data):
        if current['経常益'] is not None:
            if i + 4 < len(sorted_data):
                year_ago = sorted_data[i + 4]
                
                if year_ago['経常益'] is not None:
                    # 直近4期分のデータ（現四半期、1期前、2期前、3期前）を取得
                    recent_quarters = [current['経常益']]  # 現四半期を含める
                    for j in range(1, 4):  # 1期前〜3期前
                        if i + j < len(sorted_data) and sorted_data[i + j]['経常益'] is not None:
                            recent_quarters.append(sorted_data[i + j]['経常益'])
                        else:
                            break
                    
                    if len(recent_quarters) == 4:
                        # 分母を計算（現四半期＋直近3期の絶対値の合計）
                        denominator = sum(abs(value) for value in recent_quarters)
                        
                        if denominator != 0:
                            # 成長率を計算（パーセント表記）
                            numerator = current['経常益'] - year_ago['経常益']
                            growth_rate = (numerator / denominator) * 100
                            current['四半期成長率'] = round(growth_rate, 2)
```

**実装ポイント**:
- 正確な成長率計算式の実装（分母に現四半期を含む）
- 売上高と経常益の両方に対応
- 欠損データの適切な処理

#### 3.4 経常益利回り計算

```python
# 経常益利回りを計算
for fiscal_year, quarters in fiscal_year_data.items():
    cumulative_ordinary_income = 0
    
    for q in ['1Q', '2Q', '3Q', '4Q']:
        if q in quarters and quarters[q]['経常益'] is not None:
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
```

**計算ロジック**:
- 1Q: 年換算（×4）で利回り計算
- 2Q: 半期累計を年換算（×2）
- 3Q: 9ヶ月累計を年換算（×1.33）
- 4Q: 通期実績での利回り

#### 3.5 CSV出力処理

```python
def save_to_csv(data: List[Dict], code: str = "9984", fiscal_year_end_month: int = 3):
    # 四半期成長率と経常益利回りを計算
    data_with_growth = calculate_qoq_growth_rate(data, fiscal_year_end_month)
    
    df = pd.DataFrame(data_with_growth)
    
    # 不要な列を削除
    columns_to_drop = ['営業益', '最終益', '修正1株益']
    df = df.drop(columns=columns_to_drop, errors='ignore')
    
    # 列の順番を指定（株価データと相関を追加）
    column_order = ['決算期', '四半期', '売上高', '経常益', '発表日', 'PDF_URL', '資産合計', '資本合計', 
                   '売上高成長率', '四半期成長率', '経常益利回り', '四半期割安率_四半期平均', 
                   '四半期割安率_前年同期ベース', '四半期割安率_前四半期', '株価日付', '始値', 
                   '四半期成長率株価相関', '経常益利回り株価相関']
    df = df[column_order]
    
    # 数値列の型変換
    numeric_columns = ['売上高', '経常益', '資産合計', '資本合計', '売上高成長率', '四半期成長率', '経常益利回り']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 株価データを取得
    weekly_data = fetch_weekly_stock_data(code)
    
    # 各行に株価データを追加
    for data in data_with_growth:
        announcement_date = data.get('発表日')
        if announcement_date:
            stock_data = find_stock_price_after_announcement(weekly_data, announcement_date)
            data['株価日付'] = stock_data.get('日付')
            data['始値'] = stock_data.get('始値')
    
    # 相関計算（最新3四半期のデータを使用）
    correlations = calculate_stock_correlations(data_with_growth)
    
    # 最新の四半期にのみ相関値を設定
    if data_with_growth:
        latest_data = data_with_growth[0]
        latest_data['四半期成長率株価相関'] = correlations.get('growth_correlation')
        latest_data['経常益利回り株価相関'] = correlations.get('yield_correlation')
    
    # CSV出力（UTF-8 BOM付き）
    filename = f"quarterly_data_{code}.csv"
    output_path = f"data/output/{filename}"
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
```

**実装ポイント**:
- 四半期成長率と経常益利回りの自動計算
- 不要列の除去（営業益、最終益、修正1株益）
- 最適化された列順序
- pandas DataFrame で型安全な処理
- UTF-8 BOMでExcel互換性確保

#### 3.6 エラーハンドリング戦略

```python
# PDF処理エラー
try:
    pdf_content = download_pdf(data['PDF_URL'])
    if pdf_content:
        balance_data = extract_balance_sheet_data(pdf_content)
        data['資産合計'] = balance_data.get('資産合計')
        data['資本合計'] = balance_data.get('資本合計')
    else:
        print(f"  エラー: PDFのダウンロードに失敗")
except Exception as e:
    print(f"  エラー: PDF処理中に例外が発生 - {e}")
```

**エラー戦略**:
- **继续処理**: 一部エラーでも他のデータは正常処理
- **詳細ログ**: エラー原因を特定可能なメッセージ
- **グレースフル・デグラデーション**: 部分的な結果でも有用

## パフォーマンス最適化

### 1. メモリ管理

```python
# PDFデータをストリーム処理
with pdfplumber.open(pdf_content) as pdf:
    # ファイルハンドルの自動クローズ
    
# 大きなデータはジェネレーターで処理
def process_large_data():
    for item in large_dataset:
        yield process_item(item)
```

### 2. ネットワーク最適化

```python
# セッションの再利用
session = requests.Session()
session.headers.update(headers)

# レート制限
time.sleep(1)  # 1秒間隔
```

### 4. 株価データ取得処理

```python
def fetch_weekly_stock_data(code: str = "9984") -> List[Dict]:
    """株探の週足データを複数ページから取得"""
    all_data = []
    
    for page in [1, 2]:
        url = f"https://kabutan.jp/stock/kabuka?code={code}&ashi=wek&page={page}"
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'lxml')
        
        # 過去データテーブル（stock_kabuka_dwm）
        past_data_table = soup.find('table', class_='stock_kabuka_dwm')
        if past_data_table:
            # データ抽出処理
            
        # 今週データテーブル（stock_kabuka0）
        current_week_table = soup.find('table', class_='stock_kabuka0')
        if current_week_table:
            # 最新週データ抽出処理
```

**実装ポイント**:
- 2つのHTMLテーブルから株価データを取得（過去データ+今週データ）
- YY/MM/DD形式の日付を自動でYYYY/MM/DD形式に変換
- 複数ページ（page=1, page=2）から約60レコードを取得
- 重複データの自動除去

### 3. データ処理最適化

```python
# pandasでベクトル化処理
df[col] = pd.to_numeric(df[col], errors='coerce')

# 正規表現のコンパイル
date_pattern = re.compile(r'(\d{2}\.\d{2}-\d{2})')
```

## テスト戦略

### 1. 単体テスト対象

- `parse_balance_number()`: 数値解析ロジック
- `extract_quarterly_data()`: データ抽出ロジック
- PDF URL変換処理

### 2. 統合テスト

- エンドツーエンドでのデータ取得
- 実際のWebサイト・PDFとの連携

### 3. エラーケーステスト

- ネットワークエラー
- PDF解析失敗
- データ欠損シナリオ

## 設定・拡張性

### 1. 設定可能項目

```python
# メイン関数での設定
code = "9984"  # 証券コード変更可能
timeout = 120   # タイムアウト設定
output_dir = "data/output/"  # 出力ディレクトリ
```

### 2. 拡張ポイント

- **複数銘柄対応**: コードリストでバッチ処理
- **データ範囲拡張**: 過去データの追加取得
- **出力形式**: JSON、Excelなど他形式対応
- **決算月対応**: 任意の決算月（3月以外）への動的対応
- **成長率計算**: 四半期成長率と経常益利回りの自動計算
- **財務指標拡張**: ROE、ROAなど他の財務指標の計算
- **株価データ統合**: 週足データ取得と発表日ベース株価マッチング（実装済み）
- **相関分析**: 財務指標と株価の相関計算（実装済み）

## セキュリティ考慮

### 1. 入力検証

```python
# 証券コード検証
if not code.isdigit() or len(code) != 4:
    raise ValueError("Invalid stock code")
```

### 2. HTTP安全性

- HTTPSのみ使用
- 適切なUser-Agent設定
- セッション管理でクッキー保護

### 3. ファイル処理安全性

- パス制限（data/output配下のみ）
- ファイル名サニタイズ
- 一時ファイルの適切な削除

## 今後の改善案

### 1. 機能拡張

- 複数銘柄の並列処理
- 増分更新（新しいデータのみ取得）
- リアルタイム監視

### 2. 技術改善

- 非同期I/O（asyncio）での高速化
- キャッシュ機能
- 設定ファイル外部化

### 3. 運用改善

- ロギング強化
- 監視・アラート機能
- 自動実行スケジューリング