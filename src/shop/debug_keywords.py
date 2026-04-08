#!/usr/bin/env python3
"""
调试关键字解析
"""
import pandas as pd
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
excel_path = os.path.join(script_dir, "knowledge", "table_descriptions_optimized.xlsx")

df = pd.read_excel(excel_path, sheet_name='关键字说明')

print("="*70)
print("关键字说明 sheet 所有行:")
print("="*70)

rows_data = []
for idx, row in df.iterrows():
    col0 = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
    col1 = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ''
    col0 = '' if col0 == 'nan' else col0
    col1 = '' if col1 == 'nan' else col1
    rows_data.append((col0, col1))
    print(f"{idx:3d}: |{col0}|{col1}|")

# 找到新增业务关键字部分
print("\n" + "="*70)
print("找到新增业务关键字部分:")
print("="*70)
new_section_start = -1
for i, (k, v) in enumerate(rows_data):
    if '新增业务关键字' in k:
        new_section_start = i
        print(f"找到新增业务关键字在第 {i} 行")
        break

if new_section_start >= 0:
    print(f"\n从第 {new_section_start+1} 行开始的内容:")
    current_category = None
    for i in range(new_section_start + 1, min(new_section_start + 50, len(rows_data))):
        k, v = rows_data[i]
        print(f"{i:3d}: |{k}|{v}|")

        # 检查分类标题
        if k and not v:
            if '业务实体关键字' in k or k == '【业务实体关键字】':
                current_category = 'business_entities'
                print(f"  → 设置分类: {current_category}")
            elif '字段名关键字' in k or k == '【字段名关键字】':
                current_category = 'field_names'
                print(f"  → 设置分类: {current_category}")
            elif '状态值含义' in k or k == '【状态值含义】':
                current_category = 'status_values'
                print(f"  → 设置分类: {current_category}")
            elif '业务流程关键字' in k or k == '【业务流程关键字】':
                current_category = 'business_processes'
                print(f"  → 设置分类: {current_category}")
            continue
