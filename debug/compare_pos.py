# -*- coding: utf-8 -*-
"""
Created on Mon Jun 16 12:45:14 2025

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
import pandas as pd
import numpy as np

old_twap_path = 'D:/crypto/prod/alpha/portfolio_management/verify/roll_from_his_backtest/bt/pos/pos_merge_agg_250318_double3m_15d_73__to_00125_maxmulti_2_mm_03_pf_001_count_funding.parquet'
new_twap_path = 'D:/crypto/prod/alpha/portfolio_management/verify/roll_from_his_backtest/bt/pos_new/pos_merge_agg_250318_double3m_15d_73__to_00125_maxmulti_2_mm_03_pf_001_count_funding.parquet'

old_twap = pd.read_parquet(old_twap_path)
new_twap = pd.read_parquet(new_twap_path)

old_twap_re = old_twap.reindex(index=new_twap.index, columns=new_twap.columns)

diff = new_twap - old_twap_re

# 方法1：找出所有不相等的位置
def find_differences(df1, df2, tolerance=1e-10):
    """
    找出两个DataFrame之间的差异
    
    Parameters:
    df1, df2: pandas DataFrame
    tolerance: 数值比较的容差
    """
    # 检查数值列的差异（考虑浮点数精度）
    numeric_cols = df1.select_dtypes(include=[np.number]).columns
    non_numeric_cols = df1.select_dtypes(exclude=[np.number]).columns
    
    # 初始化差异mask
    diff_mask = pd.DataFrame(False, index=df1.index, columns=df1.columns)
    
    # 数值列比较（使用容差）
    for col in numeric_cols:
        if col in df2.columns:
            # 处理NaN值
            nan_mask1 = pd.isna(df1[col])
            nan_mask2 = pd.isna(df2[col])
            
            # NaN不匹配的情况
            nan_diff = nan_mask1 != nan_mask2
            
            # 数值差异（排除NaN）
            valid_mask = ~nan_mask1 & ~nan_mask2
            numeric_diff = valid_mask & (np.abs(df1[col] - df2[col]) > tolerance)
            
            diff_mask[col] = nan_diff | numeric_diff
    
    # 非数值列比较
    for col in non_numeric_cols:
        if col in df2.columns:
            diff_mask[col] = df1[col] != df2[col]
    
    return diff_mask

# 方法2：获取有差异的行索引
def get_diff_rows(df1, df2, tolerance=1e-10):
    """获取有差异的行索引"""
    diff_mask = find_differences(df1, df2, tolerance)
    # 找出至少有一个值不同的行
    diff_rows = diff_mask.any(axis=1)
    return df1.index[diff_rows]

# 方法3：详细对比差异
def compare_differences(df1, df2, tolerance=1e-10, max_rows=20):
    """
    详细对比两个DataFrame的差异
    
    Parameters:
    df1, df2: pandas DataFrame
    tolerance: 数值比较容差
    max_rows: 最大显示行数
    """
    diff_mask = find_differences(df1, df2, tolerance)
    diff_row_indices = get_diff_rows(df1, df2, tolerance)
    
    print(f"总共发现 {len(diff_row_indices)} 行存在差异")
    
    if len(diff_row_indices) == 0:
        print("两个DataFrame完全相同！")
        return
    
    # 显示差异统计
    print("\n差异列统计:")
    diff_cols = diff_mask.any(axis=0)
    for col in df1.columns[diff_cols]:
        diff_count = diff_mask[col].sum()
        print(f"  {col}: {diff_count} 行存在差异")
    
    # 显示前几行差异详情
    print(f"\n前 {min(max_rows, len(diff_row_indices))} 行差异详情:")
    print("="*80)
    
    for i, idx in enumerate(diff_row_indices[:max_rows]):
        print(f"\n行索引: {idx}")
        print("-"*40)
        
        # 找出这一行中有差异的列
        row_diff_cols = diff_mask.loc[idx, diff_mask.loc[idx]]
        
        for col in row_diff_cols.index:
            old_val = df1.loc[idx, col]
            new_val = df2.loc[idx, col]
            print(f"  {col}:")
            print(f"    旧值: {old_val}")
            print(f"    新值: {new_val}")
            
            # 如果是数值，显示差异
            if pd.api.types.is_numeric_dtype(df1[col]) and not pd.isna(old_val) and not pd.isna(new_val):
                diff_val = new_val - old_val
                print(f"    差异: {diff_val}")

# 方法4：创建差异报告DataFrame
def create_diff_report(df1, df2, tolerance=1e-10):
    """创建差异报告DataFrame"""
    diff_row_indices = get_diff_rows(df1, df2, tolerance)
    
    if len(diff_row_indices) == 0:
        return pd.DataFrame()
    
    # 创建差异报告
    diff_data = []
    diff_mask = find_differences(df1, df2, tolerance)
    
    for idx in diff_row_indices:
        row_diff_cols = diff_mask.loc[idx, diff_mask.loc[idx]]
        
        for col in row_diff_cols.index:
            diff_data.append({
                'row_index': idx,
                'column': col,
                'old_value': df1.loc[idx, col],
                'new_value': df2.loc[idx, col],
                'difference': df2.loc[idx, col] - df1.loc[idx, col] if pd.api.types.is_numeric_dtype(df1[col]) else 'N/A'
            })
    
    return pd.DataFrame(diff_data)

# 使用示例：
print("开始对比分析...")

# 1. 基本差异检查
diff_row_indices = get_diff_rows(old_twap_re, new_twap)
print(f"发现 {len(diff_row_indices)} 行存在差异")

# 2. 详细差异分析
compare_differences(old_twap_re, new_twap, tolerance=1e-10, max_rows=10)

# 3. 创建差异报告
diff_report = create_diff_report(old_twap_re, new_twap)
if not diff_report.empty:
    print(f"\n差异报告 (共 {len(diff_report)} 条差异):")
    print(diff_report.head(20))
    
    # 保存差异报告
    # diff_report.to_csv('twap_differences_report.csv', index=False)
    # print("\n差异报告已保存到 'twap_differences_report.csv'")

# 4. 数值差异的统计分析
numeric_cols = old_twap_re.select_dtypes(include=[np.number]).columns
if len(numeric_cols) > 0:
    print(f"\n数值列差异统计:")
    for col in numeric_cols:
        if col in new_twap.columns:
            # 计算非NaN值的差异
            old_vals = old_twap_re[col].dropna()
            new_vals = new_twap[col].reindex(old_vals.index).dropna()
            
            if len(old_vals) > 0 and len(new_vals) > 0:
                common_idx = old_vals.index.intersection(new_vals.index)
                if len(common_idx) > 0:
                    diff_vals = new_vals[common_idx] - old_vals[common_idx]
                    non_zero_diff = diff_vals[diff_vals != 0]
                    
                    if len(non_zero_diff) > 0:
                        print(f"\n  {col}:")
                        print(f"    差异点数: {len(non_zero_diff)}")
                        print(f"    最大差异: {non_zero_diff.max():.6f}")
                        print(f"    最小差异: {non_zero_diff.min():.6f}")
                        print(f"    平均差异: {non_zero_diff.mean():.6f}")
                        print(f"    差异标准差: {non_zero_diff.std():.6f}")

print("\n对比分析完成！")