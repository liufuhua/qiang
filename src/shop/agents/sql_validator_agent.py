#!/usr/bin/env python3
"""
SQL 验证 Agent
"""
import json
import re
from typing import Dict, Any, List

from agentscope.agent import AgentBase
from agentscope.message import Msg

from ..utils import _log, _parse_json_content
from ..skills import TableStructureSkill


class SQLValidatorAgent(AgentBase):
    """SQL 验证 Agent"""

    def __init__(
        self,
        table_skill: TableStructureSkill,
        name: str = "SQLValidator",
    ):
        super().__init__()
        self.name = name
        self.table_skill = table_skill

    async def __call__(self, msg: Msg = None) -> Msg:
        sql_result = _parse_json_content(msg.content) if msg else {}

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

        # 解析 SQL 中的表名和字段名
        used_tables = self._extract_tables(sql)
        used_columns = self._extract_columns(sql, used_tables)

        # 处理表别名
        alias_map = self._extract_table_aliases(sql)
        _log(f"  [{self.name}] 表别名映射: {alias_map}")

        # 将别名转换为真实表名
        real_used_tables = []
        for t in used_tables:
            real_t = alias_map.get(t, t)
            if real_t not in real_used_tables:
                real_used_tables.append(real_t)

        _log(f"  [{self.name}] SQL 中使用的真实表: {real_used_tables}")

        # 检查表是否存在
        for table_name in real_used_tables:
            if not self.table_skill.table_exists(table_name):
                is_valid = False
                validation_message = f"表不存在: {table_name}"
                needs_regenerate = True
                regenerate_hint = f"请检查表名 {table_name} 是否正确"
                break

        # 检查字段是否存在（仅当表存在时）
        if is_valid:
            for table_name in real_used_tables:
                table_columns = self.table_skill.get_table_columns(table_name)
                # 查找属于这个表的字段
                for col in used_columns:
                    # 如果字段有表别名前缀，检查是否匹配
                    if '.' in col:
                        alias, col_name = col.split('.', 1)
                        real_table = alias_map.get(alias, alias)
                        if real_table == table_name:
                            if col_name not in table_columns:
                                is_valid = False
                                validation_message = f"字段不存在: {col} (表 {table_name})"
                                needs_regenerate = True
                                columns_str = ', '.join(table_columns)
                                regenerate_hint = f"字段 {col_name} 不存在于表 {table_name} 中。该表可用字段: {columns_str}"
                                break

        # 单表查询时，补充校验无表前缀字段，减少幻觉字段漏检
        if is_valid and len(real_used_tables) == 1:
            table_name = real_used_tables[0]
            table_columns = set(self.table_skill.get_table_columns(table_name))
            ignore_identifiers = set(real_used_tables) | set(alias_map.keys())
            unqualified_columns = self._extract_unqualified_columns(sql, ignore_identifiers)
            invalid_columns = [c for c in unqualified_columns if c not in table_columns]
            if invalid_columns:
                bad_col = invalid_columns[0]
                is_valid = False
                validation_message = f"字段不存在: {bad_col} (表 {table_name})"
                needs_regenerate = True
                columns_str = ', '.join(sorted(table_columns))
                regenerate_hint = f"字段 {bad_col} 不存在于表 {table_name} 中。该表可用字段: {columns_str}"

        # 检查缺失表
        if missing_tables and is_valid:
            validation_message = f"SQL 可用，但以下表结构未知: {', '.join(missing_tables)}"

        return Msg(name=self.name, content=json.dumps({
            "is_valid": is_valid,
            "validation_message": validation_message,
            "needs_regenerate": needs_regenerate,
            "regenerate_hint": regenerate_hint,
            "sql": sql,
            "parameters": parameters,
        }, ensure_ascii=False), role="assistant")

    def _extract_tables(self, sql: str) -> List[str]:
        """从 SQL 中提取表名"""
        tables = []
        # 简单的 FROM/JOIN 解析
        sql_lower = sql.lower()

        # 查找 FROM 后面的表
        from_pattern = r'\bfrom\s+(\w+)(?:\s+(\w+))?'
        for match in re.finditer(from_pattern, sql_lower, re.IGNORECASE):
            tables.append(match.group(1))
            if match.group(2):
                tables.append(match.group(2))

        # 查找 JOIN 后面的表
        join_pattern = r'\bjoin\s+(\w+)(?:\s+(\w+))?'
        for match in re.finditer(join_pattern, sql_lower, re.IGNORECASE):
            tables.append(match.group(1))
            if match.group(2):
                tables.append(match.group(2))

        return list(dict.fromkeys(tables))

    def _extract_table_aliases(self, sql: str) -> Dict[str, str]:
        """提取表别名映射"""
        alias_map = {}
        sql_lower = sql.lower()

        # 匹配 FROM table alias 或 FROM table AS alias
        pattern = r'\b(?:from|join)\s+(\w+)(?:\s+as\s+|\s+)(\w+)\b'
        for match in re.finditer(pattern, sql_lower, re.IGNORECASE):
            real_table = match.group(1)
            alias = match.group(2)
            alias_map[alias] = real_table
            alias_map[real_table] = real_table

        return alias_map

    def _extract_columns(self, sql: str, tables: List[str]) -> List[str]:
        """从 SQL 中提取字段名"""
        columns = []
        # 简单提取 SELECT 和 WHERE 中的字段
        # 匹配 table.column 格式
        dot_pattern = r'(\w+)\.(\w+)'
        for match in re.finditer(dot_pattern, sql):
            columns.append(f"{match.group(1)}.{match.group(2)}")

        return columns

    def _extract_unqualified_columns(self, sql: str, ignore_identifiers: set) -> List[str]:
        """提取无表前缀字段名（仅用于单表 SQL 的补充校验）"""
        # 先移除字符串字面量，避免把文本内容误识别为字段
        cleaned = re.sub(r"'(?:''|[^'])*'", " ", sql)
        cleaned = re.sub(r'"(?:""|[^"])*"', " ", cleaned)
        # 移除参数占位符，避免把 %s / %(name)s 误判为字段
        cleaned = re.sub(r"%\([a-zA-Z_][a-zA-Z0-9_]*\)s", " ", cleaned)
        cleaned = re.sub(r"%s", " ", cleaned)
        # 标准化反引号标识符
        cleaned = re.sub(r"`([^`]+)`", r"\1", cleaned)
        # 去掉 table.column 形式
        cleaned = re.sub(r"\b\w+\.\w+\b", " ", cleaned)
        # 去掉 AS 别名定义，避免把别名误判为字段
        cleaned = re.sub(r"\bas\s+\w+\b", " ", cleaned, flags=re.IGNORECASE)

        tokens = re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", cleaned)

        sql_keywords_and_funcs = {
            "select", "from", "where", "and", "or", "not", "in", "is", "null",
            "between", "like", "order", "by", "group", "having", "limit", "offset",
            "join", "left", "right", "inner", "outer", "on", "as", "distinct",
            "union", "all", "case", "when", "then", "else", "end",
            "show", "describe", "explain", "with",
            "count", "sum", "avg", "min", "max", "ifnull", "coalesce",
            "date_format", "current_date", "curdate", "now",
            "year", "month", "day", "hour", "minute", "second",
            "true", "false",
        }

        result = []
        for token in tokens:
            token_lower = token.lower()
            if token_lower in sql_keywords_and_funcs:
                continue
            if token_lower in {s.lower() for s in ignore_identifiers}:
                continue
            if token_lower not in result:
                result.append(token_lower)
        return result
