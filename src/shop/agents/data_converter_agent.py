#!/usr/bin/env python3
"""
数据转换 Agent
"""
import os
import json
from datetime import datetime
import html
from typing import Dict, Any, List, Tuple

from agentscope.agent import AgentBase
from agentscope.message import Msg

from ..utils import _parse_json_content
from ..skills import TableStructureSkill


class DataConverterAgent(AgentBase):
    """数据转换 Agent"""

    def __init__(self, table_skill: TableStructureSkill, output_dir: str = None, name: str = "DataConverter"):
        super().__init__()
        self.name = name
        self.table_skill = table_skill
        self.output_dir = output_dir or os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "output")
        os.makedirs(self.output_dir, exist_ok=True)

    async def __call__(self, msg: Msg = None) -> Msg:
        input_data = _parse_json_content(msg.content) if msg else {}

        data = input_data.get("data", [])
        columns = input_data.get("columns", [])
        output_format = input_data.get("format", "json")
        task_id = input_data.get("task_id", 1)
        tables = input_data.get("tables", [])

        if not data:
            data = []

        if output_format == "json":
            content = self._to_json(data, columns)
            file_path = None
        elif output_format == "html":
            content, file_path = self._to_html(data, columns, task_id, tables)
        else:
            content = self._to_json(data, columns)
            file_path = None

        return Msg(name=self.name, content=json.dumps({
            "format": output_format,
            "content": content,
            "file_path": file_path,
        }, ensure_ascii=False, default=str), role="assistant")

    def _to_json(self, data: List[Dict], columns: List[str]) -> str:
        """转换为 JSON 格式"""
        return json.dumps({
            "columns": columns,
            "data": data,
            "count": len(data),
        }, ensure_ascii=False, indent=2, default=str)

    def _to_html(self, data: List[Dict], columns: List[str], task_id: int, tables: List[str]) -> Tuple[str, str]:
        """转换为 HTML 格式"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"result_task{task_id}_{timestamp}.html"
        file_path = os.path.join(self.output_dir, filename)

        # 构建字段名到中文说明的映射
        column_comments = {}
        for col in columns:
            column_comments[col] = col  # 默认用字段名本身

        # 从表结构中获取字段注释
        if tables:
            # 优先从 members 表获取注释（因为这是会员查询）
            # 然后从其他表获取
            table_order = list(tables)
            # 如果有 members 表，移到最前面
            if 'members' in table_order:
                table_order.remove('members')
                table_order.insert(0, 'members')

            for table_name in table_order:
                if table_name in self.table_skill.table_schemas:
                    for field in self.table_skill.table_schemas[table_name]:
                        field_name = field.get('name', '')
                        field_comment = field.get('comment', '')
                        if field_name and field_comment:
                            # 只在尚未有注释时设置，避免覆盖
                            if field_name not in column_comments or column_comments[field_name] == field_name:
                                column_comments[field_name] = field_comment

        # 确定要显示的列：如果有 _text 字段，用 _text 字段替换原字段
        display_columns = []
        text_field_map = {}  # 原字段名 -> _text 字段名
        # 先找出所有 _text 字段
        for col in columns:
            if col.endswith("_text"):
                original_col = col[:-5]
                text_field_map[original_col] = col
        # 构建显示列
        for col in columns:
            if col.endswith("_text"):
                continue  # _text 字段不单独显示，会在原字段位置显示
            if col in text_field_map:
                # 有对应的 _text 字段，标记使用 _text 字段
                display_columns.append(col)
            else:
                # 普通字段，直接显示
                display_columns.append(col)

        # 构建 HTML
        html_content = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>查询结果 - 任务 {task_id}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            padding: 20px;
        }}
        h1 {{
            color: #333;
            border-bottom: 2px solid #007bff;
            padding-bottom: 10px;
        }}
        .info {{
            color: #666;
            margin-bottom: 20px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 12px;
            text-align: left;
        }}
        th {{
            background-color: #007bff;
            color: white;
            font-weight: bold;
        }}
        tr:nth-child(even) {{
            background-color: #f8f9fa;
        }}
        tr:hover {{
            background-color: #e9ecef;
        }}
        .null-value {{
            color: #999;
            font-style: italic;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>查询结果</h1>
        <div class="info">
            <p>任务 ID: {task_id}</p>
            <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p>共 {len(data)} 条记录</p>
        </div>
"""

        if data:
            html_content += "        <table>\n"
            html_content += "            <thead>\n                <tr>\n"
            for col in display_columns:
                display_name = column_comments.get(col, col)
                html_content += f"                    <th>{html.escape(display_name)}</th>\n"
            html_content += "                </tr>\n            </thead>\n"
            html_content += "            <tbody>\n"

            for row in data:
                html_content += "                <tr>\n"
                for col in display_columns:
                    # 优先使用 _text 字段的值
                    value = None
                    text_col = f"{col}_text"
                    if text_col in row and row.get(text_col) is not None:
                        value = row.get(text_col)
                    else:
                        value = row.get(col)

                    if value is None:
                        html_content += '                    <td><span class="null-value">NULL</span></td>\n'
                    else:
                        html_content += f"                    <td>{html.escape(str(value))}</td>\n"
                html_content += "                </tr>\n"

            html_content += "            </tbody>\n"
            html_content += "        </table>\n"
        else:
            html_content += "        <p style='color: #666; font-style: italic;'>暂无数据</p>\n"

        html_content += """    </div>
</body>
</html>
"""

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

        return html_content, file_path
