#!/usr/bin/env python3
"""
运行 SQL Agent 系统的简化示例
"""
import json
import yaml
import os
import re
from typing import Dict, Any, List, Optional

# 先不初始化 agentscope，只使用知识文件


class SimpleSQLAgent:
    """简化的 SQL Agent，不依赖 AgentScope"""

    def __init__(self, knowledge_dir: str, structures_xlsx_path: str):
        self.knowledge_dir = knowledge_dir
        self.structures_xlsx_path = structures_xlsx_path
        self._load_knowledge()
        self._load_table_structures()

    def _load_knowledge(self):
        """加载知识文件"""
        # 加载工作流
        workflow_path = os.path.join(self.knowledge_dir, "workflow_by_name.json")
        with open(workflow_path, 'r', encoding='utf-8') as f:
            self.workflows = json.load(f)

        # 加载表信息
        table_info_path = os.path.join(self.knowledge_dir, "table_info.json")
        with open(table_info_path, 'r', encoding='utf-8') as f:
            self.table_info = json.load(f)

        # 加载关键字
        keywords_path = os.path.join(self.knowledge_dir, "keywords.json")
        with open(keywords_path, 'r', encoding='utf-8') as f:
            self.keywords = json.load(f)

    def _load_table_structures(self):
        """加载表结构"""
        import pandas as pd
        self.table_schemas = {}

        if os.path.exists(self.structures_xlsx_path):
            try:
                df = pd.read_excel(self.structures_xlsx_path)
                for _, row in df.iterrows():
                    table_name = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
                    schema = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ''
                    if table_name and table_name != 'nan':
                        self.table_schemas[table_name] = schema
            except Exception as e:
                print(f"Warning: Could not load table structures: {e}")

    def analyze_query(self, user_query: str) -> Dict[str, Any]:
        """Step 1: 分析用户问题"""
        query_lower = user_query.lower()

        # 判断是否需要查询数据库
        greetings = ['你好', '您好', 'hi', 'hello', '嗨', '在吗']
        if any(g in query_lower for g in greetings) and len(user_query) < 10:
            return {
                "needs_database": False,
                "friendly_reply": "你好！有什么可以帮你的吗？我可以帮你查询订单、商品、会员等数据。",
            }

        # 提取查询意图和参数
        query_intent = user_query
        key_parameters = []

        # 简单的参数提取
        # 提取年份
        year_match = re.search(r'(\d{4})年', user_query)
        if year_match:
            key_parameters.append(year_match.group(1))

        # 提取月份
        month_match = re.search(r'(\d{1,2})月', user_query)
        if month_match:
            key_parameters.append(month_match.group(1))

        return {
            "needs_database": True,
            "query_intent": query_intent,
            "key_parameters": key_parameters,
        }

    def retrieve_workflow_and_tables(self, query_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Step 2: 检索工作流和表"""
        query_intent = query_analysis.get("query_intent", "").lower()

        matched_workflow = None
        matched_tables = []

        # 关键字匹配表
        table_keywords = {
            "订单": ["member_order", "member_order_item"],
            "抽奖": ["activity", "activity_goods", "activity_item"],
            "商品": ["goods_info_new_test"],
            "会员": ["members"],
            "用户": ["members"],
            "经销商": ["dealers"],
            "渠道": ["channel"],
            "供应商": ["channel"],
            "积分": ["point_recharge_detail", "point_consum"],
            "结算": ["settlement_batch", "settlement_detail"],
            "发票": ["invoice"],
            "coupon": ["coupon", "member_coupon"],
            "优惠券": ["coupon", "member_coupon"],
            "banner": ["banner"],
        }

        for keyword, tables in table_keywords.items():
            if keyword in query_intent:
                matched_tables.extend(tables)
                # 找对应的工作流
                for wf_name in self.workflows.keys():
                    if keyword in wf_name:
                        matched_workflow = wf_name
                        break
                break

        # 如果没有匹配到，使用默认表
        if not matched_tables:
            matched_tables = ["member_order", "members"]
            matched_workflow = None

        # 补充表说明
        tables_with_info = []
        for table_name in matched_tables:
            if table_name in self.table_info:
                tables_with_info.append({
                    "name": table_name,
                    "description": self.table_info[table_name],
                })
            else:
                tables_with_info.append({"name": table_name, "description": ""})

        return {
            "workflow_name": matched_workflow,
            "tables": matched_tables,
            "tables_with_info": tables_with_info,
            "needs_clarification": False,
            "clarification_question": "",
        }

    def generate_sql(self, query_analysis: Dict[str, Any], workflow_result: Dict[str, Any]) -> Dict[str, Any]:
        """Step 3: 生成 SQL（简化版，基于模板）"""
        query_intent = query_analysis.get("query_intent", "")
        key_parameters = query_analysis.get("key_parameters", [])
        tables = workflow_result.get("tables", [])
        tables_with_info = workflow_result.get("tables_with_info", [])

        sql = ""
        parameters = []
        missing_tables = []

        # 检查缺失的表结构
        for table_info in tables_with_info:
            table_name = table_info.get("name", "")
            if table_name not in self.table_schemas:
                missing_tables.append(table_name)

        # 基于查询意图生成 SQL 模板
        query_lower = query_intent.lower()

        if "订单" in query_lower or "order" in query_lower:
            if "member_order" in tables:
                if key_parameters:
                    # 有参数的情况
                    sql = """SELECT * FROM member_order WHERE 1=1"""
                    # 这里可以根据参数添加条件
                else:
                    sql = """SELECT id, member_id, total_price, status, create_time
                             FROM member_order
                             ORDER BY create_time DESC
                             LIMIT 20"""
        elif "会员" in query_lower or "用户" in query_lower or "member" in query_lower:
            if "members" in tables:
                sql = """SELECT id, p_code, name, phone, dealer_id, create_time
                         FROM members
                         ORDER BY create_time DESC
                         LIMIT 20"""
        elif "商品" in query_lower or "goods" in query_lower:
            if "goods_info_new_test" in tables:
                sql = """SELECT goods_id, goods_name, goods_sell_status, channel_id, create_time
                         FROM goods_info_new_test
                         WHERE goods_sell_status IN (0, 1)
                         ORDER BY create_time DESC
                         LIMIT 20"""
        else:
            # 默认 SQL
            first_table = tables[0] if tables else "member_order"
            sql = f"""SELECT * FROM {first_table} LIMIT 10"""

        return {
            "sql": sql,
            "parameters": key_parameters,
            "missing_tables": missing_tables,
        }

    def validate_sql(self, sql_result: Dict[str, Any]) -> Dict[str, Any]:
        """Step 4: 验证 SQL"""
        sql = sql_result.get("sql", "")
        parameters = sql_result.get("parameters", [])
        missing_tables = sql_result.get("missing_tables", [])

        is_valid = True
        validation_message = "SQL 验证通过"
        needs_regenerate = False
        regenerate_hint = ""

        # 检查是否是只读语句
        sql_lower = sql.lower().strip()
        allowed_starts = ("select", "with", "show", "describe", "explain")
        if not sql_lower.startswith(allowed_starts):
            is_valid = False
            validation_message = "只允许 SELECT、WITH、SHOW、DESCRIBE、EXPLAIN 语句"
            needs_regenerate = True
            regenerate_hint = "请生成只读查询语句"

        # 检查是否有写入操作
        blocked = (" insert ", " update ", " delete ", " drop ", " alter ", " truncate ", " create ")
        sql_with_space = f" {sql_lower} "
        for kw in blocked:
            if kw in sql_with_space:
                is_valid = False
                validation_message = f"禁止写入操作: {kw.strip()}"
                needs_regenerate = True
                regenerate_hint = "请移除写入操作，只使用只读查询"
                break

        # 检查缺失表
        if missing_tables:
            validation_message = f"SQL 可用，但以下表结构未知: {', '.join(missing_tables)}"

        return {
            "is_valid": is_valid,
            "validation_message": validation_message,
            "needs_regenerate": needs_regenerate,
            "regenerate_hint": regenerate_hint,
            "sql": sql,
            "parameters": parameters,
        }

    def run(self, user_query: str) -> Dict[str, Any]:
        """运行完整流程"""
        print("="*70)
        print(f"用户问题: {user_query}")
        print("="*70)

        # Step 1: 问题解析
        print("\n[Step 1] 问题解析...")
        query_analysis = self.analyze_query(user_query)
        print(f"  需要数据库: {query_analysis.get('needs_database', False)}")

        if not query_analysis.get("needs_database", True):
            print(f"  友好回复: {query_analysis.get('friendly_reply', '')}")
            return {
                "needs_database": False,
                "friendly_reply": query_analysis.get("friendly_reply", ""),
            }

        print(f"  查询意图: {query_analysis.get('query_intent', '')}")
        print(f"  关键参数: {query_analysis.get('key_parameters', [])}")

        # Step 2: 工作流与表检索
        print("\n[Step 2] 检索工作流和表...")
        workflow_result = self.retrieve_workflow_and_tables(query_analysis)

        if workflow_result.get("needs_clarification", False):
            print(f"  需要澄清: {workflow_result.get('clarification_question', '')}")
            return {
                "needs_clarification": True,
                "clarification_question": workflow_result.get("clarification_question", ""),
            }

        print(f"  工作流: {workflow_result.get('workflow_name', '无')}")
        tables = workflow_result.get("tables", [])
        print(f"  相关表: {', '.join(tables) if tables else '无'}")

        # Step 3: SQL 生成
        print("\n[Step 3] 生成 SQL...")
        sql_result = self.generate_sql(query_analysis, workflow_result)
        missing_tables = sql_result.get("missing_tables", [])
        if missing_tables:
            print(f"  注意: 以下表结构未知 - {', '.join(missing_tables)}")

        # Step 4: SQL 验证
        print("\n[Step 4] 验证 SQL...")
        validation_result = self.validate_sql(sql_result)

        if validation_result.get("is_valid", False):
            print(f"  ✓ {validation_result.get('validation_message', '')}")
            print(f"\n最终结果:")
            print(f"  SQL: {validation_result.get('sql', '')}")
            print(f"  参数: {validation_result.get('parameters', [])}")
            return {
                "needs_database": True,
                "query_analysis": query_analysis,
                "workflow_result": workflow_result,
                "sql": validation_result.get("sql", ""),
                "parameters": validation_result.get("parameters", []),
                "validation_message": validation_result.get("validation_message", ""),
            }
        else:
            print(f"  ✗ {validation_result.get('validation_message', '')}")
            return {
                "needs_database": True,
                "query_analysis": query_analysis,
                "workflow_result": workflow_result,
                "error": validation_result.get("validation_message", ""),
            }


def main():
    """主函数"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    knowledge_dir = os.path.join(script_dir, "knowledge")
    structures_xlsx = os.path.join(knowledge_dir, "table_structures.xlsx")

    agent = SimpleSQLAgent(knowledge_dir, structures_xlsx)

    # 示例查询
    queries = [
        "你好",
        "查询2026年1月的订单",
        "查询会员信息",
        "查询商品信息",
    ]

    for query in queries:
        result = agent.run(query)
        print("\n" + "="*70 + "\n")


if __name__ == '__main__':
    main()
