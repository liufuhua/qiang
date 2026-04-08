#!/usr/bin/env python3
"""
数据转义 Agent - 将数据库值转换为前端显示的中文
使用 LLM 进行转换
"""
import json
from typing import Dict, Any, List

from agentscope.agent import AgentBase
from agentscope.message import Msg
from agentscope.model import ChatModelBase

from ..utils import _log, _parse_json_content, _call_model
from ..skills import TableStructureSkill


class DataTransformAgent(AgentBase):
    """数据转义 Agent - 使用 LLM 进行转换"""

    def __init__(self, table_skill: TableStructureSkill, model: ChatModelBase, name: str = "DataTransform"):
        super().__init__()
        self.name = name
        self.table_skill = table_skill
        self.model = model

    def _build_table_structure_prompt(self, tables: List[str], columns: List[str]) -> str:
        """构建表结构的 prompt"""
        prompt_parts = []
        for table_name in tables:
            if table_name in self.table_skill.table_schemas:
                prompt_parts.append(f"\n表名: {table_name}")
                prompt_parts.append("字段信息:")
                for field in self.table_skill.table_schemas[table_name]:
                    field_name = field.get('name', '')
                    if field_name in columns:
                        comment = field.get('comment', '')
                        field_type = field.get('type', '')
                        prompt_parts.append(f"  - {field_name} ({field_type}): {comment}")
        return "\n".join(prompt_parts)

    async def __call__(self, msg: Msg = None) -> Msg:
        input_data = _parse_json_content(msg.content) if msg else {}

        data = input_data.get("data", [])
        columns = input_data.get("columns", [])
        tables = input_data.get("tables", [])
        sql = input_data.get("sql", "")

        if not data:
            return Msg(name=self.name, content=json.dumps({
                "original_data": [],
                "transformed_data": [],
                "columns": columns,
            }, ensure_ascii=False, default=str), role="assistant")

        # 构建 prompt
        table_structure = self._build_table_structure_prompt(tables, columns)

        prompt = f"""你是一个数据转换专家。请根据表结构信息，将数据库查询结果中的编码值转换为中文显示。

表结构信息：
{table_structure}

执行的 SQL：
{sql}

原始数据（JSON 格式）：
{json.dumps(data, ensure_ascii=False, indent=2)}

请按以下要求处理：
1. 仔细检查表结构中每个字段的备注（comment），将所有编码值（如 0、1、2 等）转换为对应的中文描述
2. 对于 sex、job_status、position_status、type、del_flag、active、status 等状态字段，务必检查是否有对应的编码映射
3. 保持原始字段不变，为需要转换的字段添加新字段，新字段名为原字段名加 "_text" 后缀
4. 只对有明确编码映射的字段进行转换，没有映射的字段保持原样
5. 返回格式必须是严格的 JSON，格式如下：
{{
    "transformed_data": [
        {{
            "原始字段1": "原始值1",
            "原始字段1_text": "中文描述1",
            "原始字段2": "原始值2",
            ...
        }},
        ...
    ]
}}

只返回 JSON，不要有其他说明文字。"""

        _log(f"  [{self.name}] 调用 LLM 进行数据转换...")

        # 构建 messages 格式
        messages = [
            {"role": "user", "content": prompt},
        ]

        # 调用 LLM
        response = await _call_model(self.model, messages)
        result = _parse_json_content(response)

        transformed_data = result.get("transformed_data", data)

        _log(f"  [{self.name}] 数据转换完成")

        return Msg(name=self.name, content=json.dumps({
            "original_data": data,
            "transformed_data": transformed_data,
            "columns": columns,
        }, ensure_ascii=False, default=str), role="assistant")
