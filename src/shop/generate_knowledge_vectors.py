#!/usr/bin/env python3
"""
从优化后的 Excel 生成向量数据和工作流格式化文件
1. tableInfo 与 关键字说明 生成向量数据
2. workFlow 的 业务流,说明 两列生成向量数据
3. 生成可通过业务流名称访问的格式化文件
"""
import pandas as pd
import os
import json
import yaml
from typing import Dict, List, Any
from collections import defaultdict

try:
    import chromadb
    from chromadb.utils import embedding_functions
    HAS_CHROMADB = True
except ImportError:
    HAS_CHROMADB = False
    print("Warning: chromadb not available, will only generate JSON/YAML files")


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(script_dir, "knowledge", "table_descriptions_optimized.xlsx")
    output_dir = os.path.join(script_dir, "knowledge")

    # 读取三个 sheet
    print("正在读取 Excel 文件...")
    df_table_info = pd.read_excel(input_path, sheet_name='tableInfo')
    df_workflow = pd.read_excel(input_path, sheet_name='workFlow')
    df_keywords = pd.read_excel(input_path, sheet_name='关键字说明')

    # ========== 1. 处理 tableInfo ==========
    print("处理 tableInfo...")
    table_info_data = process_table_info(df_table_info)

    # ========== 2. 处理关键字说明 ==========
    print("处理关键字说明...")
    keywords_data = process_keywords(df_keywords)

    # ========== 3. 处理 workFlow ==========
    print("处理 workFlow...")
    workflow_data = process_workflow(df_workflow)

    # ========== 4. 生成向量数据 (如果有 chromadb) ==========
    if HAS_CHROMADB:
        print("生成向量数据...")
        generate_vectors(output_dir, table_info_data, keywords_data, workflow_data)

    # ========== 5. 生成格式化文件 ==========
    print("生成格式化文件...")

    # tableInfo JSON
    table_info_json = os.path.join(output_dir, "table_info.json")
    with open(table_info_json, 'w', encoding='utf-8') as f:
        json.dump(table_info_data, f, ensure_ascii=False, indent=2)
    print(f"  → {table_info_json}")

    # 关键字说明 JSON
    keywords_json = os.path.join(output_dir, "keywords.json")
    with open(keywords_json, 'w', encoding='utf-8') as f:
        json.dump(keywords_data, f, ensure_ascii=False, indent=2)
    print(f"  → {keywords_json}")

    # workFlow 格式化文件 - JSON (按名称访问)
    workflow_json = os.path.join(output_dir, "workflow_by_name.json")
    with open(workflow_json, 'w', encoding='utf-8') as f:
        json.dump(workflow_data['by_name'], f, ensure_ascii=False, indent=2)
    print(f"  → {workflow_json}")

    # workFlow 格式化文件 - YAML
    workflow_yaml = os.path.join(output_dir, "workflow_by_name.yaml")
    with open(workflow_yaml, 'w', encoding='utf-8') as f:
        yaml.dump(workflow_data['by_name'], f, allow_unicode=True, default_flow_style=False, sort_keys=False)
    print(f"  → {workflow_yaml}")

    # workFlow 列表格式
    workflow_list_json = os.path.join(output_dir, "workflow_list.json")
    with open(workflow_list_json, 'w', encoding='utf-8') as f:
        json.dump(workflow_data['list'], f, ensure_ascii=False, indent=2)
    print(f"  → {workflow_list_json}")

    # ========== 6. 生成合并的知识文件 ==========
    merged_knowledge = {
        "table_info": table_info_data,
        "keywords": keywords_data,
        "workflows": workflow_data['by_name'],
    }
    merged_json = os.path.join(output_dir, "merged_knowledge.json")
    with open(merged_json, 'w', encoding='utf-8') as f:
        json.dump(merged_knowledge, f, ensure_ascii=False, indent=2)
    print(f"  → {merged_json}")

    print("\n完成!")
    print(f"\n统计:")
    print(f"  - 表数量: {len(table_info_data)}")
    print(f"  - 关键字数量: {len(keywords_data)}")
    print(f"  - 工作流数量: {len(workflow_data['by_name'])}")


def process_table_info(df: pd.DataFrame) -> Dict[str, str]:
    """处理 tableInfo sheet"""
    table_info = {}
    for _, row in df.iterrows():
        table_name = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
        table_desc = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ''
        if table_name and table_name != 'nan':
            table_info[table_name] = table_desc if table_desc != 'nan' else ''
    return table_info


def process_keywords(df: pd.DataFrame) -> Dict[str, Any]:
    """处理关键字说明 sheet"""
    keywords = {
        "original": [],
        "business_entities": {},
        "field_names": {},
        "status_values": {},
        "business_processes": {},
    }

    rows_data = []
    for idx, row in df.iterrows():
        col0 = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
        col1 = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ''
        col0 = '' if col0 == 'nan' else col0
        col1 = '' if col1 == 'nan' else col1
        if col0:
            keywords["original"].append({"keyword": col0, "description": col1})
            rows_data.append((col0, col1))

    # 按关键字内容分类
    for k, v in rows_data:
        if not k or not v:
            continue

        # 业务实体关键字（包含"指 ... 表里的"这种模式）
        entity_patterns = ['指 members', '指 dealers', '指 company', '指 goods', '指 brands',
                          '指 channel', '指 member_order', '指 coupon', '指 activity',
                          '指 banner', '指 on_sale', '指积分', '指结算', '指发票', '指购物车']
        if any(p in v for p in entity_patterns) or k in ['会员', '用户', '经销商', '公司', '商品', '品牌', '渠道', '订单', '子订单', '优惠券', '活动', 'Banner', '秒杀', '积分', '结算', '发票', '购物车']:
            keywords["business_entities"][k] = v
        # 字段名关键字（常见字段名模式）
        elif any(suffix in k for suffix in ['Id', '_id', 'Code', '_code', 'Time', '_time', 'At', '_at', 'status', 'type', 'isDeleted', 'is_deleted', 'delFlag', 'del_flag', 'remark']):
            keywords["field_names"][k] = v
        # 状态值含义（包含=号）
        elif '=' in k:
            keywords["status_values"][k] = v
        # 业务流程关键字（动词开头）
        elif k in ['中奖', '发货', '加购', '选品', '红冲', '冲票', '上下架', '比价', '转赠', '拆单', '换货', '结算', '充值', '消费', '退款', '审核', '确认', '申请']:
            keywords["business_processes"][k] = v

    return keywords


def process_workflow(df: pd.DataFrame) -> Dict[str, Any]:
    """处理 workFlow sheet，生成可通过业务流名称访问的数据结构"""
    workflows_by_name = {}
    workflows_list = []

    current_wf = None
    current_desc = []
    current_tables = []
    current_table_conditions = {}

    for idx, row in df.iterrows():
        # 获取各列
        col_business = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
        col_desc = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ''
        col_table = str(row.iloc[2]).strip() if len(row) > 2 and pd.notna(row.iloc[2]) else ''
        col_table_desc = str(row.iloc[3]).strip() if len(row) > 3 and pd.notna(row.iloc[3]) else ''

        # 清理 nan
        col_business = '' if col_business == 'nan' else col_business
        col_desc = '' if col_desc == 'nan' else col_desc
        col_table = '' if col_table == 'nan' else col_table
        col_table_desc = '' if col_table_desc == 'nan' else col_table_desc

        # 如果业务流列有值，说明是新流程的开始
        if col_business and not col_business.startswith('='):
            # 保存之前的 workflow
            if current_wf:
                workflow_data = {
                    "name": current_wf,
                    "description": '\n'.join(current_desc).strip(),
                    "tables": current_tables,
                    "table_conditions": current_table_conditions,
                }
                workflows_by_name[current_wf] = workflow_data
                workflows_list.append(workflow_data)

            # 开始新的 workflow
            current_wf = col_business
            current_desc = []
            current_tables = []
            current_table_conditions = {}

        if current_wf:
            # 处理说明
            if col_desc:
                current_desc.append(col_desc)

            # 处理涉及表和表说明
            if col_table:
                # 清理表名
                table_name = col_table.replace('`', '').strip()
                if table_name:
                    if table_name not in current_tables:
                        current_tables.append(table_name)
                    if col_table_desc:
                        current_table_conditions[table_name] = col_table_desc

    # 保存最后一个 workflow
    if current_wf:
        workflow_data = {
            "name": current_wf,
            "description": '\n'.join(current_desc).strip(),
            "tables": current_tables,
            "table_conditions": current_table_conditions,
        }
        workflows_by_name[current_wf] = workflow_data
        workflows_list.append(workflow_data)

    return {
        "by_name": workflows_by_name,
        "list": workflows_list,
    }


def generate_vectors(
    output_dir: str,
    table_info_data: Dict[str, str],
    keywords_data: Dict[str, Any],
    workflow_data: Dict[str, Any],
):
    """生成向量数据到 ChromaDB"""
    chroma_dir = os.path.join(output_dir, "chroma_db")
    os.makedirs(chroma_dir, exist_ok=True)

    client = chromadb.PersistentClient(path=chroma_dir)

    # 1. tableInfo 向量集合
    try:
        client.delete_collection("table_info")
    except Exception:
        pass
    table_coll = client.create_collection("table_info")

    table_docs = []
    table_ids = []
    table_metadatas = []
    for idx, (table_name, table_desc) in enumerate(table_info_data.items()):
        doc = f"表名: {table_name}\n说明: {table_desc}"
        table_docs.append(doc)
        table_ids.append(f"table_{idx}")
        table_metadatas.append({"type": "table", "table_name": table_name})
    if table_docs:
        table_coll.add(documents=table_docs, ids=table_ids, metadatas=table_metadatas)

    # 2. 关键字向量集合
    try:
        client.delete_collection("keywords")
    except Exception:
        pass
    keyword_coll = client.create_collection("keywords")

    keyword_docs = []
    keyword_ids = []
    keyword_metadatas = []
    idx = 0

    # 业务实体关键字
    for kw, desc in keywords_data.get("business_entities", {}).items():
        doc = f"关键字: {kw}\n说明: {desc}"
        keyword_docs.append(doc)
        keyword_ids.append(f"kw_be_{idx}")
        keyword_metadatas.append({"type": "keyword", "category": "business_entity", "keyword": kw})
        idx += 1

    # 字段名关键字
    for kw, desc in keywords_data.get("field_names", {}).items():
        doc = f"关键字: {kw}\n说明: {desc}"
        keyword_docs.append(doc)
        keyword_ids.append(f"kw_fn_{idx}")
        keyword_metadatas.append({"type": "keyword", "category": "field_name", "keyword": kw})
        idx += 1

    # 状态值含义
    for kw, desc in keywords_data.get("status_values", {}).items():
        doc = f"关键字: {kw}\n说明: {desc}"
        keyword_docs.append(doc)
        keyword_ids.append(f"kw_sv_{idx}")
        keyword_metadatas.append({"type": "keyword", "category": "status_value", "keyword": kw})
        idx += 1

    # 业务流程关键字
    for kw, desc in keywords_data.get("business_processes", {}).items():
        doc = f"关键字: {kw}\n说明: {desc}"
        keyword_docs.append(doc)
        keyword_ids.append(f"kw_bp_{idx}")
        keyword_metadatas.append({"type": "keyword", "category": "business_process", "keyword": kw})
        idx += 1

    if keyword_docs:
        keyword_coll.add(documents=keyword_docs, ids=keyword_ids, metadatas=keyword_metadatas)

    # 3. workFlow 向量集合
    try:
        client.delete_collection("workflows")
    except Exception:
        pass
    workflow_coll = client.create_collection("workflows")

    workflow_docs = []
    workflow_ids = []
    workflow_metadatas = []
    for idx, wf in enumerate(workflow_data.get("list", [])):
        doc = f"业务流: {wf['name']}\n说明: {wf['description']}\n涉及表: {', '.join(wf['tables'])}"
        workflow_docs.append(doc)
        workflow_ids.append(f"wf_{idx}")
        workflow_metadatas.append({
            "type": "workflow",
            "name": wf['name'],
            "tables": ', '.join(wf['tables']),
        })
    if workflow_docs:
        workflow_coll.add(documents=workflow_docs, ids=workflow_ids, metadatas=workflow_metadatas)

    print(f"  → ChromaDB 向量已保存到: {chroma_dir}")


if __name__ == '__main__':
    main()
