#!/usr/bin/env python3
"""
基于 AgentScope 1.0.17 的 SQL 生成多 Agent 系统
使用真实 LLM，支持关联表查询
"""
import json
import os
import re
import asyncio
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass

import agentscope
from agentscope.agent import AgentBase
from agentscope.message import Msg
from agentscope.model import ChatModelBase


# ========== 全局配置 ==========
# 控制是否打印详细日志
_VERBOSE = False


def set_verbose(verbose: bool):
    """设置是否打印详细日志"""
    global _VERBOSE
    _VERBOSE = verbose


def _log(*args, **kwargs):
    """打印日志（仅当 _VERBOSE 为 True 时）"""
    if _VERBOSE:
        print(*args, **kwargs)


# ========== 数据类 ==========

@dataclass
class QueryAnalysis:
    """问题解析结果"""
    needs_database: bool
    query_intent: str = ""
    key_parameters: List[str] = None
    friendly_reply: str = ""


@dataclass
class WorkflowRetrievalResult:
    """工作流检索结果"""
    workflow_name: Optional[str]
    tables: List[str]
    table_descriptions: Dict[str, str]
    needs_clarification: bool = False
    clarification_question: str = ""


@dataclass
class SQLGenerationResult:
    """SQL生成结果"""
    sql: str
    parameters: List[Any]
    missing_tables: List[str] = None


@dataclass
class ValidationResult:
    """验证结果"""
    is_valid: bool
    validation_message: str
    needs_regenerate: bool = False
    regenerate_hint: str = ""
    sql: str = ""
    parameters: List[Any] = None


# ========== 工具函数 ==========

def _parse_json_content(content: Any) -> Dict[str, Any]:
    """解析 JSON 内容"""
    if isinstance(content, dict):
        return content
    if isinstance(content, str):
        try:
            # 清理 markdown 代码块标记
            cleaned = content.strip()
            if cleaned.startswith('```json'):
                cleaned = cleaned[7:]
            elif cleaned.startswith('```'):
                cleaned = cleaned[3:]
            if cleaned.endswith('```'):
                cleaned = cleaned[:-3]
            return json.loads(cleaned.strip())
        except Exception:
            return {}
    return {}


async def _call_model(model, messages: List[Dict[str, Any]]) -> str:
    """调用模型并获取完整响应"""
    # 首先尝试 monkeypatch dashscope 库来避免这个问题
    try:
        import dashscope.common.utils
        if hasattr(dashscope.common.utils, '_handle_aiohttp_failed_response'):
            orig_fn = dashscope.common.utils._handle_aiohttp_failed_response

            def patched_fn(response):
                try:
                    return orig_fn(response)
                except AttributeError as e:
                    if "'StreamReader' object has no attribute 'decode'" in str(e):
                        return '{}'
                    raise

            dashscope.common.utils._handle_aiohttp_failed_response = patched_fn
    except (ImportError, AttributeError):
        pass

    # 现在尝试调用模型
    response = None
    try:
        # 尝试传入 stream=False
        try:
            response = await model(messages, stream=False)
        except (TypeError, ValueError):
            # 如果不支持 stream 参数，使用默认方式
            response = await model(messages)
    except Exception as e:
        _log(f"  [_call_model] 模型调用失败: {e}")

    _log(f"  [_call_model] LLM 响应: {response}")

    # 尝试从 response 中提取文本
    if response is not None:
        # 情况1: OpenAIChatModel 返回的 ChatResponse 格式
        # 格式: ChatResponse(content=[{'type': 'text', 'text': '...'}])
        try:
            if hasattr(response, 'content'):
                content = response.content
                if isinstance(content, list):
                    # content 是列表，提取 text 字段
                    full_text = []
                    for item in content:
                        if isinstance(item, dict):
                            if item.get('type') == 'text':
                                full_text.append(item.get('text', ''))
                    if full_text:
                        return ''.join(full_text)
                elif isinstance(content, str):
                    return content
        except Exception:
            pass

        try:
            # 首先尝试检查是否有 text 属性
            if hasattr(response, 'text'):
                return response.text
        except Exception:
            pass

        try:
            # 检查是否是非流式响应（没有 __aiter__）
            is_async_generator = False
            try:
                is_async_generator = hasattr(response, '__aiter__')
            except Exception:
                # 如果检查 __aiter__ 时出错，认为不是 async generator
                is_async_generator = False

            if not is_async_generator:
                return str(response)
        except Exception:
            pass

        try:
            # 如果是流式响应，尝试处理
            full_response = []
            async for chunk in response:
                if hasattr(chunk, 'text'):
                    full_response.append(chunk.text)
                elif hasattr(chunk, 'content'):
                    content = chunk.content
                    if isinstance(content, str):
                        full_response.append(content)
                    elif isinstance(content, list):
                        for item in content:
                            if isinstance(item, dict) and item.get('type') == 'text':
                                full_response.append(item.get('text', ''))
                else:
                    full_response.append(str(chunk))

            final_text = ''.join(full_response)
            if final_text.strip():
                return final_text
        except Exception as e:
            _log(f"  [_call_model] 处理流式响应错误: {e}")

    # 如果所有方法都失败，返回一个默认响应
    return json.dumps({
        "needs_database": True,
        "query_intent": "查询数据",
        "key_parameters": [],
        "query_count": 1,
        "tasks": [{
            "task_id": 1,
            "query_intent": "查询数据",
            "key_parameters": [],
            "original_query": ""
        }]
    }, ensure_ascii=False)


# ========== Skill 1: 工作流与表检索 Skill ==========

class WorkflowTableSkill:
    """工作流与表检索 Skill"""

    def __init__(self, knowledge_dir: str):
        self.knowledge_dir = knowledge_dir
        self._load_knowledge()

    def _load_knowledge(self):
        """加载知识库"""
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

    def find_keywords(self, query: str) -> List[Dict]:
        """找到匹配的关键字说明"""
        matched = []
        query_lower = query.lower()
        for keyword, description in self.keywords.items():
            keyword_lower = keyword.lower()
            if keyword_lower:
                for k in keyword_lower.split(","):
                    if k.strip() in query_lower:
                        matched.append({"keyword": keyword, "description": description})
        return matched

    def find_workflow(self, query: str) -> Optional[Dict]:
        """找到匹配的工作流"""
        query_lower = query.lower()
        for wf_name, wf_data in self.workflows.items():
            if wf_name.lower() in query_lower:
                return wf_data
            # 也检查工作流描述
            desc = wf_data.get("description", "").lower()
            if desc and any(kw in query_lower for kw in desc.split()):
                return wf_data
        return None

    def get_tables_from_workflow(self, workflow: Dict) -> List[str]:
        """从工作流获取表列表"""
        return workflow.get("tables", [])

    def get_table_descriptions(self, table_names: List[str]) -> Dict[str, str]:
        """获取表说明"""
        result = {}
        for table_name in table_names:
            if table_name in self.table_info:
                result[table_name] = self.table_info[table_name]
        return result

    def get_table_conditions_from_workflow(self, workflow: Dict) -> Dict[str, str]:
        """从工作流获取表的详细条件说明"""
        return workflow.get("table_conditions", {})


# ========== Skill 2: 表结构获取 Skill ==========

class TableStructureSkill:
    """表结构获取 Skill"""

    def __init__(self, structures_xlsx_path: str):
        self.structures_xlsx_path = structures_xlsx_path
        self.table_schemas = {}
        self._load_table_structures()

    def _load_table_structures(self):
        """加载表结构"""
        import pandas as pd
        if os.path.exists(self.structures_xlsx_path):
            try:
                df = pd.read_excel(self.structures_xlsx_path)
                # 格式：表名, 字段, 类型, 可空, 索引, 备注
                for _, row in df.iterrows():
                    table_name = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
                    field_name = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ''
                    field_type = str(row.iloc[2]).strip() if len(row) > 2 and pd.notna(row.iloc[2]) else ''
                    nullable = str(row.iloc[3]).strip() if len(row) > 3 and pd.notna(row.iloc[3]) else ''
                    index_flag = str(row.iloc[4]).strip() if len(row) > 4 and pd.notna(row.iloc[4]) else ''
                    comment = str(row.iloc[5]).strip() if len(row) > 5 and pd.notna(row.iloc[5]) else ''

                    if table_name and table_name != 'nan' and field_name:
                        if table_name not in self.table_schemas:
                            self.table_schemas[table_name] = []
                        # 添加字段信息
                        field_info = {
                            "name": field_name,
                            "type": field_type,
                            "nullable": nullable,
                            "index": index_flag,
                            "comment": comment
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
            if field['comment']:
                parts.append(f"备注:{field['comment']}")
            if parts:
                line += f" ({', '.join(parts)})"
            lines.append(line)
        return "\n".join(lines)

    def get_all_schemas(self, table_names: List[str]) -> Dict[str, str]:
        """获取多个表的结构（格式化字符串）"""
        result = {}
        for table_name in table_names:
            schema = self.get_table_schema(table_name)
            if schema:
                result[table_name] = schema
        return result

    def get_missing_tables(self, table_names: List[str]) -> List[str]:
        """获取缺失的表"""
        return [t for t in table_names if t not in self.table_schemas]

    def find_related_tables(self, base_tables: List[str], max_related: int = 5) -> List[str]:
        """根据基础表查找相关联的表"""
        related_tables = []
        base_table_set = set(base_tables)

        # 收集基础表的所有字段名（小写）
        base_fields = {}
        for table in base_tables:
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
                    for base_table in base_tables:
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


# ========== Agent 1: 用户问题解析与分类 ==========

class QueryParserAgent(AgentBase):
    """问题解析与分类 Agent"""

    def __init__(self, model: ChatModelBase):
        super().__init__()
        self.name = "QueryParserAgent"
        self.model = model
        self.sys_prompt = """你是一个数据库查询问题解析专家。

你的任务是：
1. 判断用户问题是否需要查询数据库
2. 如需要查询，分析用户的问题是否包含多个独立的查询
3. 如果包含多个查询，将每个查询拆分成独立的任务
4. 对每个任务，拆分为明确的查询意图和关键参数, 关键参数要提取出来, 不可编造
5. 如不需要查询，直接给出友好回复

请以 JSON 格式输出，格式如下：
{
  "needs_database": true/false,
  "friendly_reply": "友好回复（仅当needs_database=false时）",
  "tasks": [
    {
      "task_id": 1,
      "query_intent": "用户想查询什么",
      "key_parameters": ["参数1", "参数2", ...],
      "original_query": "对应的原始查询语句片段"
    }
  ]
}

示例1：用户问"你好"
返回: {
  "needs_database": false,
  "friendly_reply": "你好！有什么可以帮你的吗？",
  "tasks": []
}

示例2：用户问"查询2026年1月的订单"
返回: {
  "needs_database": true,
  "friendly_reply": "",
  "tasks": [
    {
      "task_id": 1,
      "query_intent": "查询指定时间段的订单",
      "key_parameters": ["2026年1月"],
      "original_query": "查询2026年1月的订单"
    }
  ]
}

注意：
- 每个任务都是一个独立的查询
- 任务之间用逗号、分号、句号等分隔
- key_parameters 必须从用户问题中提取，不可编造
- 如果没有明确的参数，key_parameters 可以是空数组
"""

    async def reply(
        self,
        msg: Msg | List[Msg] | None = None,
        **kwargs: Any,
    ) -> Msg:
        user_query = msg.content if msg else ""

        messages = [
            {"role": "system", "content": self.sys_prompt},
            {"role": "user", "content": f"用户问题: {user_query}"},
        ]

        _log(f"  [QueryParser] 调用 LLM...")
        _log(f"  [QueryParser] Prompt:")
        _log(f"  [QueryParser] System: {self.sys_prompt[:150]}...")
        _log(f"  [QueryParser] User: 用户问题: {user_query}")
        content = await _call_model(self.model, messages)
        _log(f"  [QueryParser] LLM 响应: {content[:300]}...")

        try:
            # 清理 markdown 代码块
            cleaned = content.strip()
            if cleaned.startswith('```json'):
                cleaned = cleaned[7:]
            elif cleaned.startswith('```'):
                cleaned = cleaned[3:]
            if cleaned.endswith('```'):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()

            json_match = re.search(r'\{[\s\S]*\}', cleaned)
            if json_match:
                cleaned = json_match.group(0)
            result = json.loads(cleaned)

            # 确保 tasks 存在
            if "tasks" not in result:
                result["tasks"] = []

            # 如果没有 tasks 但 needs_database=true，创建一个默认 task
            if result.get("needs_database", False) and not result["tasks"]:
                result["tasks"] = [{
                    "task_id": 1,
                    "query_intent": user_query,
                    "key_parameters": [],
                    "original_query": user_query
                }]

            _log(f"  [QueryParser] 解析成功: 共 {len(result.get('tasks', []))} 个任务")
            for i, task in enumerate(result.get("tasks", [])):
                _log(f"    任务 {i+1}: {task.get('query_intent', '')}")
        except Exception as e:
            _log(f"  [QueryParser] 解析失败: {e}")
            result = {
                "needs_database": True,
                "friendly_reply": "",
                "tasks": [{
                    "task_id": 1,
                    "query_intent": user_query,
                    "key_parameters": [],
                    "original_query": user_query
                }]
            }

        return Msg(name=self.name, content=json.dumps(result, ensure_ascii=False), role="assistant")


# ========== Agent 2: 向量数据库检索与工作流表检索 ==========

class WorkflowRetrieverAgent(AgentBase):
    """工作流与表检索 Agent"""

    def __init__(self, knowledge_dir: str, model: ChatModelBase):
        super().__init__()
        self.name = "WorkflowRetrieverAgent"
        self.model = model
        self.skill = WorkflowTableSkill(knowledge_dir)
        self.keyword_filter_prompt = """你是一个关键字筛选专家。

根据用户的查询意图，从给定的关键字列表中筛选出相关的关键字。

请以 JSON 格式输出：
{
  "relevant_keywords": ["关键字1", "关键字2", ...]
}

注意：
- 只从提供的关键字列表中选择
- 选择所有相关的关键字
"""
        self.workflow_filter_prompt = """你是一个工作流筛选专家。

根据用户的查询意图，从给定的工作流列表中筛选出相关的工作流。

请以 JSON 格式输出：
{
  "relevant_workflows": ["工作流1", "工作流2", ...]
}

注意：
- 只从提供的工作流列表中选择
- 选择所有相关的工作流
"""
        self.table_filter_prompt = """你是一个数据库表筛选专家。

根据用户的查询意图、关键字说明和工作流信息，从给定的表列表中选择最相关的表（最多5个）。

请以 JSON 格式输出：
{
  "tables": ["表1", "表2", ...]
}

注意：
- 只从提供的表列表中选择
- 选择最多5个最相关的表
- 优先选择主表，其次选择需要关联的表
"""

    async def reply(
        self,
        msg: Msg | List[Msg] | None = None,
        **kwargs: Any,
    ) -> Msg:
        content = msg.content if msg else "{}"
        query_analysis = _parse_json_content(content)
        query_intent = query_analysis.get("query_intent", "")

        # 检查是否有记忆数据
        memory_data = query_analysis.get("_memory", {})

        # Step 1: 获取所有关键字（从记忆或从 skill）
        _log(f"  [WorkflowRetriever] Step 1: 获取所有关键字...")
        all_keywords = memory_data.get("all_keywords")
        if all_keywords is None:
            all_keywords = self.skill.keywords.get("original", [])
            _log(f"  [WorkflowRetriever] 从 skill 加载: {len(all_keywords)} 个关键字")
        else:
            _log(f"  [WorkflowRetriever] 从记忆加载: {len(all_keywords)} 个关键字")

        # Step 2: 根据提问 + 所有的 keyword，筛选出有关系的
        _log(f"  [WorkflowRetriever] Step 2: 筛选相关关键字...")
        # 检查是否有缓存的关键字
        cached_keywords = memory_data.get("cached_keywords")
        if cached_keywords:
            _log(f"  [WorkflowRetriever] 使用缓存的关键字: {cached_keywords}")
            relevant_keywords = cached_keywords
        else:
            relevant_keywords = await self._filter_keywords(query_intent, all_keywords)
            _log(f"  [WorkflowRetriever] 筛选出 {len(relevant_keywords)} 个相关关键字 {relevant_keywords}")
        # 获取完整的关键字信息
        matched_keywords = []
        for kw in all_keywords:
            if kw.get("keyword", "") in relevant_keywords:
                matched_keywords.append(kw)

        # Step 3: 获取所有工作流（从记忆或从 skill）
        _log(f"  [WorkflowRetriever] Step 3: 获取所有工作流...")
        all_workflows = memory_data.get("all_workflows")
        if all_workflows is None:
            all_workflows = list(self.skill.workflows.items())
            _log(f"  [WorkflowRetriever] 从 skill 加载: {len(all_workflows)} 个工作流")
        else:
            _log(f"  [WorkflowRetriever] 从记忆加载: {len(all_workflows)} 个工作流")

        # Step 4: 根据提问 + 所有的工作流 + 工作流说明，筛选出所有有关系的工作流
        _log(f"  [WorkflowRetriever] Step 4: 筛选相关工作流...")
        # 检查是否有缓存的工作流
        cached_workflows = memory_data.get("cached_workflows")
        if cached_workflows:
            _log(f"  [WorkflowRetriever] 使用缓存的工作流: {cached_workflows}")
            relevant_workflow_names = cached_workflows
        else:
            relevant_workflow_names = await self._filter_workflows(query_intent, all_workflows)
            _log(f"  [WorkflowRetriever] 筛选出 {len(relevant_workflow_names)} 个相关工作流")

        # Step 5: 合并 keyword 的说明与工作流的表
        _log(f"  [WorkflowRetriever] Step 5: 合并表...")
        all_tables = set()
        workflow_name = None
        workflow_data = None
        for wf_name, wf_data in all_workflows:
            if wf_name in relevant_workflow_names:
                if not workflow_name:
                    workflow_name = wf_name
                    workflow_data = wf_data
                tables = self.skill.get_tables_from_workflow(wf_data)
                all_tables.update(tables)
        all_tables = list(all_tables)
        _log(f"  [WorkflowRetriever] 合并后共有 {len(all_tables)} 个表")

        # Step 6: 获取表说明
        table_descriptions = self.skill.get_table_descriptions(all_tables)

        # Step 7: 选出最多5个表
        if all_tables:
            _log(f"  [WorkflowRetriever] Step 7: 从 {len(all_tables)} 个表中选出最多5个...")
            all_tables = await self._filter_final_tables(query_intent, all_tables, table_descriptions, matched_keywords, relevant_workflow_names)
            # 重新获取筛选后表的说明
            table_descriptions = self.skill.get_table_descriptions(all_tables)

        # Step 8: 过滤掉不存在的表
        valid_tables = [t for t in all_tables if t in table_descriptions]

        # 获取工作流的 table_conditions
        table_conditions = {}
        if workflow_data:
            table_conditions = workflow_data.get("table_conditions", {})
            # 如果 table_conditions 里有表不在 valid_tables 中，也加进来（最多再加5个）
            extra_tables = []
            for t_name in table_conditions.keys():
                if t_name not in valid_tables and t_name in self.skill.table_info:
                    extra_tables.append(t_name)
                    if len(extra_tables) >= 5:
                        break
            if extra_tables:
                _log(f"  [WorkflowRetriever] 从 table_conditions 补充表: {extra_tables}")
                valid_tables = valid_tables + extra_tables
                # 更新表说明
                table_descriptions = self.skill.get_table_descriptions(valid_tables)

        result = {
            "workflow_name": workflow_name,
            "tables": valid_tables,
            "table_descriptions": table_descriptions,
            "matched_keywords": matched_keywords,
            "table_conditions": table_conditions,
            "needs_clarification": False,
            "clarification_question": "",
            # 返回缓存供后续任务使用
            "cached_keywords": relevant_keywords,
            "cached_workflows": relevant_workflow_names,
        }

        return Msg(name=self.name, content=json.dumps(result, ensure_ascii=False), role="assistant")

    async def _filter_keywords(self, query_intent: str, all_keywords: List[Dict]) -> List[str]:
        """Step 2: 筛选相关关键字"""
        # keyword_list_str = "\n".join([f"- {kw.get('keyword', '')}" for kw in all_keywords])

        prompt = f"""查询意图: {query_intent}

所有关键字:
{all_keywords}

请从上面的关键字中筛选出相关的关键字。
"""

        messages = [
            {"role": "system", "content": self.keyword_filter_prompt},
            {"role": "user", "content": prompt},
        ]

        _log(f"  [WorkflowRetriever] [FilterKeywords] Prompt:")
        _log(f"  [WorkflowRetriever] [FilterKeywords] User:\n{prompt}")
        content = await _call_model(self.model, messages)

        try:
            cleaned = self._clean_json(content)
            result = json.loads(cleaned)
            return result.get("relevant_keywords", [])
        except Exception as e:
            _log(f"  [WorkflowRetriever] [FilterKeywords] 解析失败: {e}")
            return []

    async def _filter_workflows(self, query_intent: str, all_workflows: List[Tuple]) -> List[str]:
        """Step 4: 筛选相关工作流"""
        workflow_list_str = []
        for wf_name, wf_data in all_workflows:
            desc = wf_data.get("description", "")
            if desc:
                workflow_list_str.append(f"- {wf_name}: {desc}")
            else:
                workflow_list_str.append(f"- {wf_name}")
        workflow_list_str = "\n".join(workflow_list_str)

        prompt = f"""查询意图: {query_intent}

所有工作流:
{workflow_list_str}

请从上面的工作流中筛选出相关的工作流。
"""

        messages = [
            {"role": "system", "content": self.workflow_filter_prompt},
            {"role": "user", "content": prompt},
        ]

        _log(f"  [WorkflowRetriever] [FilterWorkflows] Prompt:")
        _log(f"  [WorkflowRetriever] [FilterWorkflows] User:\n{prompt}")
        content = await _call_model(self.model, messages)

        try:
            cleaned = self._clean_json(content)
            result = json.loads(cleaned)
            return result.get("relevant_workflows", [])
        except Exception as e:
            _log(f"  [WorkflowRetriever] [FilterWorkflows] 解析失败: {e}")
            return []

    async def _filter_final_tables(self, query_intent: str, tables: List[str], table_descriptions: Dict[str, str],
                                  matched_keywords: List[Dict], relevant_workflows: List[str]) -> List[str]:
        """Step 7: 最终筛选表"""
        table_list_str = []
        for t in tables:
            desc = table_descriptions.get(t, "")
            if desc:
                table_list_str.append(f"- {t}: {desc}")
            else:
                table_list_str.append(f"- {t}")
        table_list_str = "\n".join(table_list_str)

        keyword_str = ""
        if matched_keywords:
            keyword_str = "\n\n关键字说明:\n"
            for kw in matched_keywords:
                keyword_str += f"- {kw.get('keyword', '')}: {kw.get('description', '')}\n"

        workflow_str = ""
        if relevant_workflows:
            workflow_str = "\n\n相关工作流:\n"
            for wf in relevant_workflows:
                workflow_str += f"- {wf}\n"

        prompt = f"""查询意图: {query_intent}
{keyword_str}
{workflow_str}
可用的表:
{table_list_str}

请从上面的信息中选择最多5个最相关的表。
"""

        messages = [
            {"role": "system", "content": self.table_filter_prompt},
            {"role": "user", "content": prompt},
        ]

        _log(f"  [WorkflowRetriever] [FilterFinalTables] Prompt:")
        _log(f"  [WorkflowRetriever] [FilterFinalTables] User:\n{prompt}")
        content = await _call_model(self.model, messages)

        try:
            cleaned = self._clean_json(content)
            result = json.loads(cleaned)
            return result.get("tables", [])
        except Exception as e:
            _log(f"  [WorkflowRetriever] [FilterFinalTables] 解析失败: {e}")
            return tables[:5]

    def _clean_json(self, content: str) -> str:
        """清理 JSON 字符串"""
        cleaned = content.strip()
        if cleaned.startswith('```json'):
            cleaned = cleaned[7:]
        elif cleaned.startswith('```'):
            cleaned = cleaned[3:]
        if cleaned.endswith('```'):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()
        json_match = re.search(r'\{[\s\S]*\}', cleaned)
        if json_match:
            cleaned = json_match.group(0)
        return cleaned


# ========== Agent 3: SQL 生成器 ==========

class SQLGeneratorAgent(AgentBase):
    """SQL 生成 Agent"""

    def __init__(self, knowledge_dir: str, structures_xlsx_path: str, model: ChatModelBase):
        super().__init__()
        self.name = "SQLGeneratorAgent"
        self.model = model
        self.table_skill = TableStructureSkill(structures_xlsx_path)
        self.workflow_skill = WorkflowTableSkill(knowledge_dir)
        self.sys_prompt = """你是一个 MySQL 5.7 SQL 生成专家。

根据用户的查询意图、关键参数和表结构，生成安全的只读 SQL。

要求：
1. 只生成 SELECT语句
2. 使用 MySQL 5.7 语法
3. 可以进行多表关联查询（JOIN）
4. 只使用提供的表结构中存在的表和字段！如果
5. 返回格式为 JSON：
   {
     "sql": "生成的 SQL 语句，使用 ? 作为参数占位符",
     "parameters": ["参数1", "参数2", ...]
   }


"""

    async def reply(
        self,
        msg: Msg | List[Msg] | None = None,
        **kwargs: Any,
    ) -> Msg:
        content = msg.content if msg else "{}"
        input_data = _parse_json_content(content)
        query_intent = input_data.get("query_intent", "")
        key_parameters = input_data.get("key_parameters", [])
        tables = input_data.get("tables", [])
        table_descriptions = input_data.get("table_descriptions", {})
        matched_keywords = input_data.get("matched_keywords", [])
        workflow_name = input_data.get("workflow_name", "")
        table_conditions = input_data.get("table_conditions", {})

        # Step 1: 获取表结构
        _log(f"  [SQLGenerator] 获取表结构...")
        table_schemas = self.table_skill.get_all_schemas(tables)
        missing_tables = self.table_skill.get_missing_tables(tables)
        _log(f"  [SQLGenerator] 缺失表: {missing_tables}")

        # 如果有缺失表，反馈给第二步
        if missing_tables:
            _log(f"  [SQLGenerator] 有缺失表，需要补充...")
            # 这里可以请求第二步补充表，但先继续使用现有表

        # Step 2: 构建提示
        schema_info = []
        for table_name in tables:
            if table_name in table_schemas:
                desc = table_descriptions.get(table_name, "")
                schema_info.append(f"表: {table_name}\n说明: {desc}\n结构:\n{table_schemas[table_name]}")
            elif table_name in table_descriptions:
                schema_info.append(f"表: {table_name}\n说明: {table_descriptions[table_name]}\n结构: 未知")

        # 构建关键字说明
        keyword_info = ""
        if matched_keywords:
            keyword_info = "\n\n关键字说明:\n"
            for kw in matched_keywords:
                keyword_info += f"- {kw.get('keyword', '')}: {kw.get('description', '')}\n"

        # 构建工作流说明，包括 table_conditions
        workflow_info = ""
        if workflow_name:
            workflow_info = f"\n\n工作流: {workflow_name}"
            # 从 workflow_skill 获取完整的工作流数据
            workflow_data = None
            if hasattr(self.workflow_skill, 'workflows'):
                workflow_data = self.workflow_skill.workflows.get(workflow_name, {})
            if workflow_data:
                wf_desc = workflow_data.get("description", "")
                if wf_desc:
                    workflow_info += f"\n工作流说明: {wf_desc}"

        # 构建表关系说明（table_conditions）
        table_relation_info = ""
        if table_conditions:
            table_relation_info = "\n\n表关系说明（table_conditions）:"
            table_relation_info += "\n【重要】这里描述了表之间的关联关系，生成 JOIN 时请参考："
            for t_name, t_cond in table_conditions.items():
                table_relation_info += f"\n  - {t_name}: {t_cond}"

        prompt = f"""查询意图: {query_intent}
关键参数: {', '.join(key_parameters) if key_parameters else '无'}{keyword_info}{workflow_info}{table_relation_info}

表信息:
{chr(10).join(schema_info)}

请生成 MySQL 5.7 的只读 SQL，支持多表关联查询。
【重要提示】
1. 如果需要关联查询，请仔细参考"表关系说明"中的表关联关系
- 仔细查看提供的表结构，只使用存在的字段名
- 使用表结构中的主键和外键进行 JOIN
- 如果参数是日期，使用正确的日期格式
- 不要编造不存在的表或字段
- 如果需要关联查询，确保使用正确的关联字段
必须严格遵守以下规则:
【主表选择规则（最高优先级）】
1. 优先把 **带有WHERE精准过滤条件** 的表作为主表（放在FROM后面第一个）。
4. 主表判断标准：WHERE 条件作用在哪个表，哪个表就是主表。
5. 先过滤主表，再关联其他表。
6. 确保查询性能最优、结果正确、不丢数据。
7. 禁止忽略WHERE条件
9. 生成SQL时，时间条件必须使用数据库标准函数：
    - 今天：current_date
   - 当月：date_format(时间字段, '%Y-%m') = date_format(current_date, '%Y-%m')
   - 今年：date_format(时间字段, '%Y') = date_format(current_date, '%Y')
10. 所有的查询都需要分页,最大20条, 分页最大第10页
"""

        messages = [
            {"role": "system", "content": self.sys_prompt},
            {"role": "user", "content": prompt},
        ]

        _log(f"  [SQLGenerator] 调用 LLM...")
        _log(f"  [SQLGenerator] Prompt:")
        _log(f"  [SQLGenerator] System: {self.sys_prompt[:100]}...")
        _log(f"  [SQLGenerator] User:\n{prompt}")
        content = await _call_model(self.model, messages)
        _log(f"  [SQLGenerator] LLM 响应: {content[:200]}...")

        try:
            # 清理 markdown 代码块
            cleaned = content.strip()
            if cleaned.startswith('```json'):
                cleaned = cleaned[7:]
            elif cleaned.startswith('```'):
                cleaned = cleaned[3:]
            if cleaned.endswith('```'):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()

            json_match = re.search(r'\{[\s\S]*\}', cleaned)
            if json_match:
                cleaned = json_match.group(0)
            result = json.loads(cleaned)
            _log(f"  [SQLGenerator] 解析成功")
        except Exception as e:
            _log(f"  [SQLGenerator] 解析失败: {e}")
            result = {"sql": "", "parameters": key_parameters if key_parameters else []}

        result["missing_tables"] = missing_tables
        return Msg(name=self.name, content=json.dumps(result, ensure_ascii=False), role="assistant")


# ========== Agent 4: SQL 验证器 ==========

class SQLValidatorAgent(AgentBase):
    """SQL 验证 Agent"""

    def __init__(self, structures_xlsx_path: str, model: ChatModelBase):
        super().__init__()
        self.name = "SQLValidatorAgent"
        self.model = model
        self.table_skill = TableStructureSkill(structures_xlsx_path)

    async def reply(
        self,
        msg: Msg | List[Msg] | None = None,
        **kwargs: Any,
    ) -> Msg:
        content = msg.content if msg else "{}"
        sql_result = _parse_json_content(content)

        validation_result = self._validate_locally(sql_result)

        return Msg(name=self.name, content=json.dumps(validation_result, ensure_ascii=False), role="assistant")

    def _extract_tables_and_aliases(self, sql: str) -> Dict[str, str]:
        """从 SQL 中提取表名和别名，返回 {别名: 真实表名}"""
        alias_map = {}
        sql_lower = sql.lower()

        # 匹配 FROM 和 JOIN 子句中的表和别名
        # 模式: FROM table [AS] alias
        # 模式: JOIN table [AS] alias
        patterns = [
            r'from\s+`?(\w+)`?(?:\s+(?:as\s+)?)?`?(\w+)`?',
            r'join\s+`?(\w+)`?(?:\s+(?:as\s+)?)?`?(\w+)`?',
        ]

        for pattern in patterns:
            matches = re.finditer(pattern, sql_lower)
            for match in matches:
                table = match.group(1)
                alias = match.group(2) if match.group(2) else table
                alias_map[alias] = table
                # 同时也记录真实表名到自身的映射
                alias_map[table] = table

        return alias_map

    def _validate_locally(self, sql_result: Dict[str, Any]) -> Dict[str, Any]:
        """本地验证 SQL"""
        sql = sql_result.get("sql", "")
        parameters = sql_result.get("parameters", [])
        missing_tables = sql_result.get("missing_tables", [])
        tables_from_input = sql_result.get("tables", [])

        is_valid = True
        validation_message = "SQL 验证通过"
        needs_regenerate = False
        regenerate_hint = ""

        # 检查是否是空 SQL
        if not sql or not sql.strip():
            is_valid = False
            validation_message = "SQL 为空"
            needs_regenerate = True
            regenerate_hint = "请生成有效的 SQL"
            return {
                "is_valid": is_valid,
                "validation_message": validation_message,
                "needs_regenerate": needs_regenerate,
                "regenerate_hint": regenerate_hint,
                "sql": sql,
                "parameters": parameters,
            }

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

        # 从 SQL 中提取表名和别名
        alias_map = self._extract_tables_and_aliases(sql)
        _log(f"  [SQLValidator] 表别名映射: {alias_map}")

        # 获取所有真实表名
        used_real_tables = list(set(alias_map.values()))
        _log(f"  [SQLValidator] SQL 中使用的真实表: {used_real_tables}")

        # 检查表是否存在
        invalid_tables = []
        for table in used_real_tables:
            if table not in self.table_skill.table_schemas:
                invalid_tables.append(table)

        if invalid_tables:
            is_valid = False
            validation_message = f"SQL 中使用了不存在的表: {', '.join(invalid_tables)}"
            needs_regenerate = True
            regenerate_hint = f"请只使用存在的表: {', '.join(sorted(self.table_skill.table_schemas.keys()))}"
            return {
                "is_valid": is_valid,
                "validation_message": validation_message,
                "needs_regenerate": needs_regenerate,
                "regenerate_hint": regenerate_hint,
                "sql": sql,
                "parameters": parameters,
            }

        # 检查字段是否存在
        invalid_fields = []

        # 查找所有 alias.field 的模式
        # 模式: alias.field
        alias_field_pattern = r'(\w+)\.(\w+)'
        alias_field_matches = re.findall(alias_field_pattern, sql_lower)

        for alias, field in alias_field_matches:
            if field == '*':
                continue  # 跳过 *
            # 查找别名对应的真实表
            real_table = alias_map.get(alias)
            if not real_table:
                continue  # 别名为找到，可能是子查询或其他，跳过
            if real_table not in self.table_skill.table_schemas:
                continue  # 表不存在，前面已经检查过了
            # 获取该表的有效字段
            valid_fields = {f['name'].lower() for f in self.table_skill.table_schemas[real_table]}
            if field not in valid_fields:
                invalid_fields.append(f"{alias}.{field} (表: {real_table})")

        if invalid_fields:
            is_valid = False
            validation_message = f"SQL 中使用了不存在的字段: {', '.join(invalid_fields)}"
            needs_regenerate = True
            # 收集可用字段信息
            field_hints = []
            for table in used_real_tables:
                if table in self.table_skill.table_schemas:
                    fields = [f['name'] for f in self.table_skill.table_schemas[table]]
                    field_hints.append(f"{table}: {', '.join(fields[:15])}{'...' if len(fields) > 15 else ''}")
            regenerate_hint = f"请只使用存在的字段。可用字段: {'; '.join(field_hints)}"
            return {
                "is_valid": is_valid,
                "validation_message": validation_message,
                "needs_regenerate": needs_regenerate,
                "regenerate_hint": regenerate_hint,
                "sql": sql,
                "parameters": parameters,
            }

        # 检查缺失表（从输入传来的）
        if missing_tables:
            validation_message = f"SQL 可用，但以下表结构未知: {', '.join(missing_tables)}"

        return {
            "is_valid": is_valid,
            "validation_message": validation_message,
            "needs_regenerate": needs_regenerate,
            "regenerate_hint": regenerate_hint,
            "sql": sql,
            "parameters": parameters,
            "used_tables": used_real_tables,
            "alias_map": alias_map,
        }


# ========== 主协调器 ==========

@dataclass
class PipelineMemory:
    """管道记忆数据类"""
    # 缓存的关键字筛选结果
    cached_keywords: Optional[List[str]] = None
    # 缓存的工作流筛选结果
    cached_workflows: Optional[List[str]] = None
    # 缓存的所有关键字
    all_keywords: Optional[List[Dict]] = None
    # 缓存的所有工作流
    all_workflows: Optional[List[Tuple]] = None


class SQLAgentPipeline:
    """SQL Agent 管道"""

    def __init__(self, knowledge_dir: str, structures_xlsx_path: str, model: ChatModelBase):
        self.query_parser = QueryParserAgent(model)
        self.workflow_retriever = WorkflowRetrieverAgent(knowledge_dir, model)
        self.sql_generator = SQLGeneratorAgent(knowledge_dir, structures_xlsx_path, model)
        self.sql_validator = SQLValidatorAgent(structures_xlsx_path, model)
        self.table_skill = TableStructureSkill(structures_xlsx_path)
        self.workflow_skill = WorkflowTableSkill(knowledge_dir)
        # 初始化记忆
        self.memory = PipelineMemory()
        # 预加载知识库到记忆
        self._preload_knowledge()

    def _preload_knowledge(self):
        """预加载知识库到记忆中"""
        _log("[Pipeline] 预加载知识库...")
        self.memory.all_keywords = self.workflow_retriever.skill.keywords.get("original", [])
        self.memory.all_workflows = list(self.workflow_retriever.skill.workflows.items())
        _log(f"[Pipeline] 预加载完成: {len(self.memory.all_keywords)} 个关键字, {len(self.memory.all_workflows)} 个工作流")

    async def _run_single_task(self, task: Dict[str, Any], max_retries: int = 3) -> Dict[str, Any]:
        """运行单个任务"""
        task_id = task.get("task_id", 1)
        query_intent = task.get("query_intent", "")
        key_parameters = task.get("key_parameters", [])
        original_query = task.get("original_query", query_intent)

        _log(f"\n{'='*70}")
        _log(f"[任务 {task_id}] {original_query}")
        _log(f"{'='*70}")

        # 构建单任务的 query_analysis
        task_query_analysis = {
            "needs_database": True,
            "query_intent": query_intent,
            "key_parameters": key_parameters,
        }

        # Step 2: 向量数据库检索与工作流表检索（使用记忆）
        _log(f"\n[任务 {task_id}] Step 1: 检索工作流和表...")
        # 将记忆传递给 retriever
        retriever_input_dict = {
            **task_query_analysis,
            "_memory": {
                "all_keywords": self.memory.all_keywords,
                "all_workflows": self.memory.all_workflows,
                "cached_keywords": self.memory.cached_keywords,
                "cached_workflows": self.memory.cached_workflows,
            }
        }
        retriever_input = json.dumps(retriever_input_dict, ensure_ascii=False)
        retriever_msg = await self.workflow_retriever(Msg(name="QueryParserAgent", content=retriever_input, role="user"))
        workflow_result = _parse_json_content(retriever_msg.content)

        # 更新记忆
        if "cached_keywords" in workflow_result:
            self.memory.cached_keywords = workflow_result["cached_keywords"]
        if "cached_workflows" in workflow_result:
            self.memory.cached_workflows = workflow_result["cached_workflows"]

        if workflow_result.get("needs_clarification", False):
            _log(f"  需要澄清: {workflow_result.get('clarification_question', '')}")
            return {
                "task_id": task_id,
                "original_query": original_query,
                "needs_clarification": True,
                "clarification_question": workflow_result.get("clarification_question", ""),
            }

        _log(f"  工作流: {workflow_result.get('workflow_name', '无')}")
        tables = workflow_result.get("tables", [])
        _log(f"  相关表: {', '.join(tables) if tables else '无'}")

        # Step 3 & 4: SQL 生成与验证（带重试）
        _log(f"\n[任务 {task_id}] Step 2 & 3: 生成并验证 SQL...")
        generator_input_dict = {
            **task_query_analysis,
            **workflow_result,
        }
        generator_input = json.dumps(generator_input_dict, ensure_ascii=False)

        # 记录已尝试的表组合
        tried_table_combinations = [frozenset(tables)]
        # 记录 SQL 中实际使用的表（用于查找关联表）
        sql_used_tables = []

        for attempt in range(max_retries):
            _log(f"  尝试 {attempt + 1}/{max_retries}...")

            # 生成 SQL
            generator_msg = await self.sql_generator(Msg(name="WorkflowRetrieverAgent", content=generator_input, role="user"))
            sql_result = _parse_json_content(generator_msg.content)

            # 验证 SQL
            validator_input = json.dumps(sql_result, ensure_ascii=False)
            validator_msg = await self.sql_validator(Msg(name="SQLGeneratorAgent", content=validator_input, role="user"))
            validation_result = _parse_json_content(validator_msg.content)

            if validation_result.get("is_valid", False):
                _log(f"  ✓ {validation_result.get('validation_message', '')}")
                _log(f"\n  任务 {task_id} 结果:")
                _log(f"  SQL: {validation_result.get('sql', '')}")
                _log(f"  参数: {validation_result.get('parameters', [])}")
                return {
                    "task_id": task_id,
                    "original_query": original_query,
                    "success": True,
                    "query_intent": query_intent,
                    "key_parameters": key_parameters,
                    "workflow_result": workflow_result,
                    "sql": validation_result.get("sql", ""),
                    "parameters": validation_result.get("parameters", []),
                    "validation_message": validation_result.get("validation_message", ""),
                }
            else:
                _log(f"  ✗ {validation_result.get('validation_message', '')}")
                if validation_result.get("needs_regenerate", False) and attempt < max_retries - 1:
                    _log(f"  重新生成...")

                    # 记录 SQL 中使用的表
                    used_tables = validation_result.get("used_tables", [])
                    if used_tables:
                        sql_used_tables = used_tables
                        _log(f"  [Pipeline] SQL 中使用的表: {sql_used_tables}")

                    # 查找关联表
                    if sql_used_tables:
                        current_tables = generator_input_dict.get("tables", [])
                        related_tables = self.table_skill.find_related_tables(sql_used_tables, max_related=5)
                        _log(f"  [Pipeline] 找到关联表: {related_tables}")

                        # 合并表，去重
                        new_tables = list(set(current_tables + related_tables))
                        new_table_set = frozenset(new_tables)

                        # 确保不重复尝试相同的表组合
                        if new_table_set not in tried_table_combinations and len(new_tables) > len(current_tables):
                            _log(f"  [Pipeline] 补充表后: {new_tables}")
                            tried_table_combinations.append(new_table_set)

                            # 更新 generator_input_dict
                            generator_input_dict["tables"] = new_tables
                            # 更新表说明
                            table_descriptions = self.workflow_skill.get_table_descriptions(new_tables)
                            generator_input_dict["table_descriptions"] = table_descriptions

                    generator_input_dict["regenerate_hint"] = validation_result.get("regenerate_hint", "")
                    generator_input = json.dumps(generator_input_dict, ensure_ascii=False)
                else:
                    break

        _log(f"\n任务 {task_id} 未能生成有效的 SQL")
        return {
            "task_id": task_id,
            "original_query": original_query,
            "success": False,
            "query_intent": query_intent,
            "key_parameters": key_parameters,
            "workflow_result": workflow_result,
            "error": "无法生成有效的 SQL",
        }

    async def run(self, user_query: str, max_retries: int = 3) -> Dict[str, Any]:
        """运行完整流程"""
        _log("="*70)
        _log(f"用户问题: {user_query}")
        _log("="*70)

        # Step 1: 问题解析与分类
        _log("\n[Step 1] 问题解析与分类...")
        parser_msg = await self.query_parser(Msg(name="user", content=user_query, role="user"))
        query_analysis = _parse_json_content(parser_msg.content)
        _log(f"  需要数据库: {query_analysis.get('needs_database', False)}")

        if not query_analysis.get("needs_database", True):
            _log(f"  友好回复: {query_analysis.get('friendly_reply', '')}")
            return {
                "needs_database": False,
                "friendly_reply": query_analysis.get("friendly_reply", ""),
                "tasks": [],
            }

        # 获取任务列表
        tasks = query_analysis.get("tasks", [])
        if not tasks:
            # 如果没有任务，创建一个默认任务
            tasks = [{
                "task_id": 1,
                "query_intent": user_query,
                "key_parameters": [],
                "original_query": user_query
            }]

        _log(f"\n共 {len(tasks)} 个任务需要执行")

        # 逐个执行任务
        task_results = []
        for task in tasks:
            task_result = await self._run_single_task(task, max_retries)
            task_results.append(task_result)

        # 汇总结果
        _log(f"\n{'='*70}")
        _log("所有任务执行完成")
        _log(f"{'='*70}")
        for i, result in enumerate(task_results):
            status = "✓" if result.get("success", False) else "✗"
            _log(f"{status} 任务 {result.get('task_id', i+1)}: {result.get('original_query', '')}")

        return {
            "needs_database": True,
            "original_query": user_query,
            "task_count": len(task_results),
            "tasks": task_results,
        }


def _patch_dashscope():
    """修复 dashscope 库的 StreamReader decode 错误"""
    try:
        # 首先尝试修复最底层的问题
        import dashscope.common.utils

        # 保存原始函数
        original_handle_response = dashscope.common.utils._handle_aiohttp_failed_response

        def patched_handle_response(response):
            """修复 StreamReader decode 错误的补丁"""
            try:
                # 检查是否有 content 属性
                if hasattr(response, 'content'):
                    content = response.content

                    # 判断 content 类型
                    # 情况1: 已经是 str 类型
                    if isinstance(content, str):
                        pass  # 已经是 str，无需处理
                    # 情况2: 是 bytes 类型
                    elif isinstance(content, bytes):
                        pass  # 已经是 bytes，可以 decode
                    # 情况3: 是 StreamReader (有 read 方法但没有 decode)
                    elif hasattr(content, 'read') and not hasattr(content, 'decode'):
                        # 是 StreamReader，返回空 JSON 来避免错误
                        return '{}'
            except Exception:
                pass

            # 尝试调用原始函数，但捕获特定异常
            try:
                return original_handle_response(response)
            except AttributeError as e:
                if "'StreamReader' object has no attribute 'decode'" in str(e):
                    # 捕获特定的 StreamReader decode 错误
                    return '{}'
                raise
            except Exception:
                # 其他异常，返回空 JSON
                return '{}'

        # 应用补丁
        dashscope.common.utils._handle_aiohttp_failed_response = patched_handle_response
        _log("[Main] Applied dashscope patch")

        # 同时也尝试修复其他可能的问题
        try:
            # 尝试修改 aiohttp 响应处理
            import aiohttp
            if hasattr(aiohttp, 'StreamReader'):
                # 给 StreamReader 添加一个 decode 方法
                original_read = aiohttp.StreamReader.read

                async def read_with_decode(self, n=-1):
                    data = await original_read(self, n)
                    return data

                aiohttp.StreamReader.read = read_with_decode
                # 添加 decode 方法
                def decode(self, encoding='utf-8'):
                    return ''

                aiohttp.StreamReader.decode = decode
                _log("[Main] Added StreamReader.decode patch")
        except (ImportError, AttributeError):
            pass

        return True
    except (ImportError, AttributeError) as e:
        _log(f"[Main] Could not patch dashscope: {e}")
        return False


async def main():
    """主函数"""
    import sys
    # 检查是否有 -v 或 --verbose 参数
    verbose = '-v' in sys.argv or '--verbose' in sys.argv
    set_verbose(verbose)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    knowledge_dir = os.path.join(script_dir, "knowledge")
    structures_xlsx = os.path.join(knowledge_dir, "table_structures.xlsx")

    from dotenv import load_dotenv
    load_dotenv()

    # 先修复 dashscope 库（以防万一）
    _patch_dashscope()

    api_key = os.getenv("OPENAI_API_KEY", "dummy")
    # DashScope 兼容模式模型名格式: qwen-plus, qwen-max, qwen-turbo, qwen-long 等
    model_name = os.getenv("SHOP_OLLAMA_MODEL_NAME", "qwen-max")
    # 如果模型名包含 qwen3.5，转换为 qwen-plus
    if "qwen3.5" in model_name.lower():
        model_name = "qwen-plus"

    # 使用 OpenAIChatModel 连接 DashScope 兼容模式 API
    from agentscope.model import OpenAIChatModel

    model_kwargs = {
        "model_name": model_name,
        "api_key": api_key,
    }
    # 添加 base_url
    base_url = os.getenv("SHOP_OLLAMA_BASE_URL")
    if base_url:
        model_kwargs["client_kwargs"] = {"base_url": base_url}

    model = OpenAIChatModel(**model_kwargs)

    # 尝试禁用流式响应 - 尝试多种方式
    if hasattr(model, '_default_generate_args'):
        model._default_generate_args['stream'] = False
    if hasattr(model, 'generate_args'):
        model.generate_args['stream'] = False

    # 检查并修改模型的其他属性
    for attr_name in dir(model):
        if 'stream' in attr_name.lower() and not attr_name.startswith('_'):
            try:
                setattr(model, attr_name, False)
            except Exception:
                pass

    # 打印模型属性以调试
    _log(f"[Main] Model attributes: {[attr for attr in dir(model) if not attr.startswith('_')]}")

    # 检查并修改 _model 属性
    if hasattr(model, '_model'):
        inner_model = model._model
        if hasattr(inner_model, 'generate_args'):
            inner_model.generate_args['stream'] = False
        if hasattr(inner_model, '_default_generate_args'):
            inner_model._default_generate_args['stream'] = False

    pipeline = SQLAgentPipeline(knowledge_dir, structures_xlsx, model)

    queries = [
        # "你好",
        # "查询2026年1月的订单，包括会员信息",
        # "查询会员信息和所属经销商",
        # "查询商品信息和所属渠道",
        # "查询商品编号是123的商品所属一级,二级,三级分类",
        # "查询销售代码是ABC的经销商所属大区,小区"
        "查询Pcode P128579 的充值记录"
        # "查询所有待确认订单"
        # "查询使用了RSD8政策的已支付,已发货,已完成订单"
        # "查询订单号ABCD所属的大区"
        # "查询PCode=123的会员收到赠送的积分记录"
        # "查询比亚迪公司的所有待确认订单号"  # 把奥迪认为是品牌了
        # "订单号为ABC的物流信息"
        # "查询今天的总订单数与中免GDF渠道订单数; 当前的所有待确认订单数"
    ]

    for query in queries:
        result = await pipeline.run(query)
        print("\n" + "="*70 + "\n")
        for task in result.get("tasks"):
            print(task.get("original_query", ""), task.get("sql", ""), task.get("parameters", ""))


if __name__ == '__main__':
    asyncio.run(main())

# 奥迪,大众,比亚迪,捷达 这几个公司查询时一定要加上 公司, 不然会被认为是品牌