import json
import shutil
import re
from pathlib import Path
from typing import Any, Dict, List, Union, Optional
from crewai.tools import tool

from langchain_community.utilities import SQLDatabase
from langchain_community.tools.sql_database.tool import (
    QuerySQLDatabaseTool,
    InfoSQLDatabaseTool,
    ListSQLDatabaseTool,
)
from sqlalchemy import text

DB: Optional[SQLDatabase] = None


def set_sql_db(db: SQLDatabase) -> None:
    global DB
    DB = db

@tool
def ListTablesTool(tables: Union[str, List[str], None] = None, **kwargs) -> str:
    """返回可用的业务数据库表名列表，忽略传入的参数"""
    return [
        "member_order",
        "member_order_item",
        "goods_info_new_test",
        "members",
        "channel",
        "dealers",
    ]

@tool
def PickTablesTool(query: str, candidates, limit: int = 10, **kwargs) -> str:
    """根据查询语句和候选表列表自动挑选相关的表"""
    top_n = kwargs.get("top_n")
    if top_n is not None:
        try:
            limit = int(top_n)
        except Exception:
            pass
    if isinstance(candidates, list):
        table_list = [t.strip() for t in candidates if isinstance(t, str) and t.strip()]
    else:
        table_list = [t.strip() for t in str(candidates).split(",") if t.strip()]
    table_set = set(table_list)
    limit = max(1, min(int(limit), 20))
    q = query.lower()
    rules = [
        (["经销商", "dealer", "渠道", "门店"], "dealers"),
        (["会员", "member", "用户", "客户"], "members"),
        (["公司", "企业", "company"], "company"),
        (["区域", "地区", "area", "省", "市"], "area"),
        (["订单", "order", "主订单"], "member_order"),
        (["订单商品关联", "order_item", "主订单商品", "子订单", "订单明细", "订单"], "member_order_item"),
        (["商品信息", "goods_info", "订单的商品", "商品"], "goods_info_new_test"),
        (["供应商", "channel", "商品供货方", "商品供应商", "渠道商", "渠道"], "channel"),
    ]
    picked: List[str] = []
    def add_if_exists(t: str):
        if t in table_set and t not in picked:
            picked.append(t)
    for triggers, tname in rules:
        if any(tri.lower() in q for tri in triggers):
            add_if_exists(tname)
    tokens = re.findall(r"[a-zA-Z_]+|[\u4e00-\u9fff]+|\d+", query)
    tokens = [tok.lower() for tok in tokens if tok.strip()]
    for t in table_list:
        tl = t.lower()
        if any(tok in tl or tl in tok for tok in tokens):
            add_if_exists(t)
    if not picked:
        if "members" in table_set:
            picked = ["members"]
        else:
            picked = [table_list[0]] if table_list else []
    picked = picked[:limit]
    return ", ".join(picked)

@tool
def TablesSchemaTool(tables: Union[str, List[str]]) -> str:
   """根据表名列表返回数据库表结构信息"""
   if DB is None:
       raise ValueError("SQL database is not initialized")
   db = DB
   if isinstance(tables, str):
       names = [t.strip() for t in tables.split(",") if t.strip()]
   else:
       names = [t.strip() for t in tables if t.strip()]
   result: Dict[str, Any] = {}
   unknown: List[str] = []
   usable = set(db.get_usable_table_names())
   for t in names:
       if t not in usable:
           unknown.append(t)
           continue
       info_tool = InfoSQLDatabaseTool(db=db)
       info = info_tool.invoke(t)
       result[t] = info
   if unknown:
       result["__unknown_tables__"] = unknown
   return json.dumps(result, ensure_ascii=False)


@tool
def CheckSQLTool(query: str) -> str:
   """对给定 SQL 做只读和表名校验"""
   if DB is None:
       raise ValueError("SQL database is not initialized")
   db = DB
   q = query.strip()
   q_lower = q.lower()
   allowed_starts = ("select", "with", "show", "describe", "explain")
   if not q_lower.startswith(allowed_starts):
       payload = {
           "status": "error",
           "reason": "only_read_queries",
           "message": "Only SELECT/WITH/SHOW/DESCRIBE/EXPLAIN queries are allowed.",
       }
       return json.dumps(payload, ensure_ascii=False)
   blocked = (" insert ", " update ", " delete ", " drop ", " alter ", " truncate ", " create ")
   text_with_space = f" {q_lower} "
   for kw in blocked:
       if kw in text_with_space:
           payload = {
               "status": "error",
               "reason": "write_operation",
               "message": "Write operations are not allowed.",
           }
           return json.dumps(payload, ensure_ascii=False)
   usable = set(db.get_usable_table_names())
   found_tables: List[str] = []
   patterns = [
       r"\bfrom\s+([`\[\"\]\w\.]+)",
       r"\bjoin\s+([`\[\"\]\w\.]+)",
   ]
   import re

   for pat in patterns:
       for m in re.finditer(pat, q_lower):
           t = m.group(1).strip("`\"[] ")
           if t and t not in found_tables:
               found_tables.append(t)
   unknown = [t for t in found_tables if t not in usable]
   if unknown:
       payload = {
           "status": "error",
           "reason": "unknown_table",
           "unknown_tables": unknown,
           "message": "Some tables do not exist in database.",
       }
       return json.dumps(payload, ensure_ascii=False)
   payload = {"status": "ok", "errors": [], "raw": "basic static check only"}
   return json.dumps(payload, ensure_ascii=False)


@tool
def ExecuteSQLTool(query: str) -> str:
    """在限制条件下执行只读 SQL 并返回查询结果"""
    print("####", query)
    if DB is None:
        raise ValueError("SQL database is not initialized")
    db = DB
    q = query.strip()
    q_lower = q.lower()
    allowed_starts = ("select", "with", "show", "describe", "explain")
    if not q_lower.startswith(allowed_starts):
        return json.dumps({"error": "只允许只读查询", "sql": q}, ensure_ascii=False)
    blocked = (" insert ", " update ", " delete ", " drop ", " alter ", " truncate ", " create ")
    text_with_space = f" {q_lower} "
    for kw in blocked:
        if kw in text_with_space:
            return json.dumps({"error": "禁止写入操作", "sql": q}, ensure_ascii=False)
    engine = db._engine
    with engine.connect() as conn:
        result = conn.execute(text(q))
        rows = [dict(row._mapping) for row in result]
    payload = {"sql": q, "rows": rows}
    return json.dumps(payload, ensure_ascii=False, default=str)
