#!/usr/bin/env python3
"""
SQL 执行 Agent
"""
import sys
import os
import json
from typing import Dict, Any, List, Optional, Tuple

import pymysql
from agentscope.agent import AgentBase
from agentscope.message import Msg

from ..utils import _log, _parse_json_content


class SQLExecutorAgent(AgentBase):
    """SQL 执行 Agent"""

    def __init__(self, name: str = "SQLExecutor"):
        super().__init__()
        self.name = name

    async def __call__(self, msg: Msg = None) -> Msg:
        input_data = _parse_json_content(msg.content) if msg else {}

        sql = input_data.get("sql", "")
        parameters = input_data.get("parameters", [])

        if not sql:
            return Msg(name=self.name, content=json.dumps({
                "success": False,
                "error_message": "SQL 为空",
                "needs_retry": False,
            }, ensure_ascii=False, default=str), role="assistant")

        try:
            try:
                from ..dbConfig import getDB
            except ImportError:
                from dbConfig import getDB

            conn = None
            try:
                conn = getDB()
                with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                    _log(f"  [{self.name}] 执行 SQL: {sql}")
                    _log(f"  [{self.name}] 参数: {parameters}")

                    # 确保使用 %s 作为占位符
                    sql = sql.replace('?', '%s')
                    # 确保 parameters 是 tuple 或 list
                    if not isinstance(parameters, (tuple, list)):
                        parameters = [parameters] if parameters is not None else []

                    if not parameters:
                        cursor.execute(sql)
                    else:
                        cursor.execute(sql, parameters)
                    data = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []

                    _log(f"  [{self.name}] 查询成功，返回 {len(data)} 行")

                    return Msg(name=self.name, content=json.dumps({
                        "success": True,
                        "data": data,
                        "columns": columns,
                        "error_message": "",
                        "needs_retry": False,
                    }, ensure_ascii=False, default=str), role="assistant")

            finally:
                if conn:
                    conn.close()

        except Exception as e:
            error_msg = str(e)
            _log(f"  [{self.name}] 执行错误: {error_msg}")

            # 判断是否需要重试（例如 SQL 语法错误等）
            needs_retry = any(keyword in error_msg.lower() for keyword in [
                "syntax", "column", "table", "doesn't exist", "unknown"
            ])

            return Msg(name=self.name, content=json.dumps({
                "success": False,
                "data": None,
                "columns": None,
                "error_message": error_msg,
                "needs_retry": needs_retry,
                "sql": sql,
                "parameters": parameters,
            }, ensure_ascii=False, default=str), role="assistant")
