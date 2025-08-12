# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## プロジェクト概要

株探（Kabutan）の四半期成長率計算ツール。財務データの前四半期比（QoQ）成長率を計算・分析するためのプロジェクト。

## 開発環境のセットアップ

### Python プロジェクトの場合
```bash
# 仮想環境の作成

- 必ずuvを用いること



## 財務データ処理の考慮事項

1. **データの正確性**: 金額は整数（円単位）または Decimal 型で扱う
2. **四半期の識別**: 決算期（1Q, 2Q, 3Q, 4Q）を明確に管理
3. **成長率計算**: (現四半期 - 1年前の同四半期(5期前の四半期のこと)) / sum(abs(1期前の四半期),abs(2期前の四半期),abs(3期前の四半期),abs(4期前の四半期))
4. **例外処理**: データ不足のため計算不可能として出力対応