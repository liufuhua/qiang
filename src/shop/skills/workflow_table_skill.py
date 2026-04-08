#!/usr/bin/env python3
"""
工作流与表检索 Skill
"""
import os
import json
from typing import Dict, Any, List, Tuple


class WorkflowTableSkill:
    """工作流与表检索 Skill"""

    def __init__(self, knowledge_dir: str):
        self.knowledge_dir = knowledge_dir
        self._load_knowledge()

    def _load_knowledge(self):
        """加载知识文件"""
        # 加载工作流
        workflow_path = os.path.join(self.knowledge_dir, "workflow_by_name.json")
        with open(workflow_path, 'r', encoding='utf-8') as f:
            self.workflows = json.load(f)

        # 加载关键字
        keywords_path = os.path.join(self.knowledge_dir, "keywords.json")
        with open(keywords_path, 'r', encoding='utf-8') as f:
            self.keywords = json.load(f)

        # 加载 keywords_table.json 并合并，如果 key 存在则跳过
        keywords_table_path = os.path.join(self.knowledge_dir, "keywords_table.json")
        if os.path.exists(keywords_table_path):
            with open(keywords_table_path, 'r', encoding='utf-8') as f:
                keywords_table = json.load(f)
            for k, v in keywords_table.items():
                if k not in self.keywords:
                    self.keywords[k] = v

    def get_all_keywords(self) -> List[str]:
        """获取所有关键字"""
        return list(self.keywords.keys())

    def get_all_workflows(self) -> List[Dict[str, str]]:
        """获取所有工作流"""
        result = []
        for k, v in self.workflows.items():
            if isinstance(v, dict):
                result.append({"name": k, "description": v.get("description", "")})
            else:
                result.append({"name": k, "description": str(v)})
        return result

    def get_tables_from_keywords(self, keywords: List[str]) -> List[str]:
        """从关键字获取相关表"""
        # 这里需要一个映射，暂时返回空
        return []

    def get_tables_from_workflow(self, workflow_name: str) -> Tuple[List[str], Dict[str, str]]:
        """从工作流获取相关表"""
        if workflow_name not in self.workflows:
            return [], {}

        workflow_data = self.workflows[workflow_name]
        tables = []
        table_descriptions = {}

        # 从 workflow 的 tables 字段获取
        if isinstance(workflow_data, dict):
            tables = workflow_data.get("tables", [])
            table_conditions = workflow_data.get("table_conditions", {})
            for table_name in tables:
                table_descriptions[table_name] = table_conditions.get(table_name, "")

        return tables, table_descriptions
