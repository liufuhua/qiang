#!/usr/bin/env python3
"""
表结构 Skill
"""
import os
from typing import Dict, Any, List, Optional

try:
    from ..utils import _log
except ImportError:
    from utils import _log


class TableStructureSkill:
    """表结构获取 Skill"""

    def __init__(self, structures_xlsx_path: str):
        self.structures_xlsx_path = structures_xlsx_path
        self.table_schemas = {}
        self.table_remarks = {}
        self._load_table_structures()
        self._load_table_remarks()

    def _load_table_structures(self):
        """加载表结构"""
        import pandas as pd
        if os.path.exists(self.structures_xlsx_path):
            try:
                df = pd.read_excel(self.structures_xlsx_path)
                # 新格式：表名, 字段, 类型, 可空, 索引, 列名, 转义, 说明, 关联
                for _, row in df.iterrows():
                    table_name = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
                    field_name = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ''
                    field_type = str(row.iloc[2]).strip() if len(row) > 2 and pd.notna(row.iloc[2]) else ''
                    nullable = str(row.iloc[3]).strip() if len(row) > 3 and pd.notna(row.iloc[3]) else ''
                    index_flag = str(row.iloc[4]).strip() if len(row) > 4 and pd.notna(row.iloc[4]) else ''
                    col_name = str(row.iloc[5]).strip() if len(row) > 5 and pd.notna(row.iloc[5]) else ''
                    escape = str(row.iloc[6]).strip() if len(row) > 6 and pd.notna(row.iloc[6]) else ''
                    desc = str(row.iloc[7]).strip() if len(row) > 7 and pd.notna(row.iloc[7]) else ''
                    relation = str(row.iloc[8]).strip() if len(row) > 8 and pd.notna(row.iloc[8]) else ''

                    if table_name and table_name != 'nan' and field_name:
                        if table_name not in self.table_schemas:
                            self.table_schemas[table_name] = []
                        # 添加字段信息
                        field_info = {
                            "name": field_name,
                            "type": field_type,
                            "nullable": nullable,
                            "index": index_flag,
                            "col_name": col_name,
                            "escape": escape,
                            "desc": desc,
                            "relation": relation
                        }
                        self.table_schemas[table_name].append(field_info)
            except Exception as e:
                _log(f"Warning: Could not load table structures: {e}")

    def get_table_schema(self, table_name: str) -> Optional[str]:
        """获取表结构（格式化字符串）"""
        if table_name not in self.table_schemas:
            return None
        fields = self.table_schemas[table_name]
        lines = []
        for field in fields:
            line = f"- {field['name']}: {field['type']}"
            parts = []
            if field['nullable']:
                parts.append(f"可空:{field['nullable']}")
            if field['index']:
                parts.append(f"索引:{field['index']}")
            if field['col_name']:
                parts.append(f"列名:{field['col_name']}")
            if field['escape']:
                parts.append(f"转义:{field['escape']}")
            if field['desc']:
                parts.append(f"说明:{field['desc']}")
            if field['relation']:
                parts.append(f"关联:{field['relation']}")
            if parts:
                line += f" ({', '.join(parts)})"
            lines.append(line)
        return "\n".join(lines)

    def get_all_table_schemas(self, table_names: List[str]) -> Dict[str, str]:
        """获取多个表的结构（格式化字符串）"""
        result = {}
        for table_name in table_names:
            schema = self.get_table_schema(table_name)
            if schema:
                result[table_name] = schema
        return result

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return table_name in self.table_schemas

    def get_missing_tables(self, table_names: List[str]) -> List[str]:
        """获取缺失的表"""
        return [t for t in table_names if t not in self.table_schemas]

    def find_related_tables(self, base_tables: List[str], max_related: int = 5) -> List[str]:
        """根据基础表查找相关联的表"""
        related_tables = []
        base_table_set = set(base_tables) if isinstance(base_tables, list) else {base_tables}

        # 收集基础表的所有字段名（小写）
        base_fields = {}
        for table in base_table_set:
            if table in self.table_schemas:
                for field in self.table_schemas[table]:
                    field_name_lower = field['name'].lower()
                    if field_name_lower not in base_fields:
                        base_fields[field_name_lower] = []
                    base_fields[field_name_lower].append(table)

        # 遍历所有表，找出可能关联的表
        for table_name, fields in self.table_schemas.items():
            if table_name in base_table_set:
                continue

            # 检查是否有与基础表相同的ID字段
            has_relation = False
            for field in fields:
                field_name_lower = field['name'].lower()

                # 检查是否是常见的关联字段
                if field_name_lower in base_fields:
                    has_relation = True
                    break

                # 检查字段名是否以_id结尾，且前面部分匹配某个表名
                if field_name_lower.endswith('_id'):
                    prefix = field_name_lower[:-3]
                    # 检查是否有表名包含这个前缀
                    for base_table in base_table_set:
                        if prefix in base_table.lower() or base_table.lower() in prefix:
                            has_relation = True
                            break
                    if has_relation:
                        break

            if has_relation:
                related_tables.append(table_name)
                if len(related_tables) >= max_related:
                    break

        return related_tables

    def get_table_columns(self, table_name: str) -> List[str]:
        """获取表的字段列表"""
        if table_name not in self.table_schemas:
            return []
        return [field['name'] for field in self.table_schemas[table_name]]

    def get_field_col_name(self, table_name: str, field_name: str) -> str:
        """获取字段的列名（显示名）"""
        if table_name not in self.table_schemas:
            return ""
        for field in self.table_schemas[table_name]:
            if field['name'] == field_name:
                return field.get('col_name', '')
        return ""

    def get_field_escape(self, table_name: str, field_name: str) -> str:
        """获取字段的转义信息"""
        if table_name not in self.table_schemas:
            return ""
        for field in self.table_schemas[table_name]:
            if field['name'] == field_name:
                return field.get('escape', '')
        return ""

    def get_field_desc(self, table_name: str, field_name: str) -> str:
        """获取字段的说明信息"""
        if table_name not in self.table_schemas:
            return ""
        for field in self.table_schemas[table_name]:
            if field['name'] == field_name:
                return field.get('desc', '')
        return ""

    def get_field_relation(self, table_name: str, field_name: str) -> str:
        """获取字段的关联信息"""
        if table_name not in self.table_schemas:
            return ""
        for field in self.table_schemas[table_name]:
            if field['name'] == field_name:
                return field.get('relation', '')
        return ""

    def get_all_field_info(self, table_name: str) -> List[Dict[str, str]]:
        """获取表的所有字段完整信息"""
        if table_name not in self.table_schemas:
            return []
        return self.table_schemas[table_name]

    def _load_table_remarks(self):
        """加载表备注信息"""
        import json
        remark_path = os.path.join(os.path.dirname(self.structures_xlsx_path), "remark.json")
        if os.path.exists(remark_path):
            try:
                with open(remark_path, 'r', encoding='utf-8') as f:
                    self.table_remarks = json.load(f)
            except Exception as e:
                _log(f"Warning: Could not load table remarks: {e}")

    def get_table_remark(self, table_name: str) -> str:
        """获取表的备注信息（查询时需要注意的信息）"""
        return self.table_remarks.get(table_name, "")

    def get_all_table_remarks(self, table_names: List[str]) -> Dict[str, str]:
        """获取多个表的备注信息"""
        result = {}
        for table_name in table_names:
            remark = self.get_table_remark(table_name)
            if remark:
                result[table_name] = remark
        return result
