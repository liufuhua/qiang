#!/usr/bin/env python3
"""
工作流检索 Agent
"""
import json
import re
from typing import Dict, Any, List

from agentscope.agent import AgentBase
from agentscope.message import Msg
from agentscope.model import ChatModelBase

from ..utils import _log, _parse_json_content, _call_model
from ..skills import WorkflowTableSkill


class WorkflowRetrieverAgent(AgentBase):
    """工作流与表检索 Agent"""

    def __init__(
        self,
        model: ChatModelBase,
        workflow_skill: WorkflowTableSkill,
        name: str = "WorkflowRetriever",
    ):
        super().__init__()
        self.name = name
        self.model = model
        self.workflow_skill = workflow_skill
        self.all_keywords = None
        self.all_workflows = None
        self._keyword_cache = {}
        self._workflow_cache = {}
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

    async def __call__(self, msg: Msg = None) -> Msg:
        query_analysis = _parse_json_content(msg.content) if msg else {}
        query_intent = query_analysis.get("query_intent", "")

        # Step 1: 获取所有关键字
        _log(f"  [{self.name}] Step 1: 获取所有关键字...")
        if self.all_keywords is None:
            self.all_keywords = self.workflow_skill.get_all_keywords()
            _log(f"  [{self.name}] 从 skill 加载: {len(self.all_keywords)} 个关键字")
        else:
            _log(f"  [{self.name}] 从记忆加载: {len(self.all_keywords)} 个关键字")

        # Step 2: 筛选相关关键字 - 先用字符串匹配，再用 LLM 补充
        _log(f"  [{self.name}] Step 2: 筛选相关关键字...")

        # 首先用字符串匹配找到明显相关的关键字
        query_lower = query_intent.lower()
        string_matched_keywords = []
        for keyword in self.all_keywords:
            keyword_lower = keyword.lower()
            # 支持逗号分隔的多个关键字
            for kw_part in keyword_lower.split(","):
                kw_part = kw_part.strip()
                if kw_part and kw_part in query_lower:
                    string_matched_keywords.append(keyword)
                    break

        # 去重
        string_matched_keywords = list(dict.fromkeys(string_matched_keywords))
        _log(f"  [{self.name}] 字符串匹配到的关键字: {string_matched_keywords}")

        # 再用 LLM 筛选补充
        cache_key = f"kw_{query_intent}"
        if cache_key in self._keyword_cache:
            llm_keywords = self._keyword_cache[cache_key]
            _log(f"  [{self.name}] 使用缓存的关键字: {llm_keywords}")
        else:
            llm_keywords = await self._filter_keywords(query_intent, self.all_keywords)
            self._keyword_cache[cache_key] = llm_keywords

        # 合并两种方式的结果
        relevant_keyword_names = list(dict.fromkeys(string_matched_keywords + llm_keywords))
        _log(f"  [{self.name}] 合并后的关键字: {relevant_keyword_names}")

        # 构建完整的 matched_keywords 列表（包含 description）
        matched_keywords = []
        for kw_name in relevant_keyword_names:
            if kw_name in self.workflow_skill.keywords:
                matched_keywords.append({
                    "keyword": kw_name,
                    "description": self.workflow_skill.keywords[kw_name]
                })

        _log(f"  [{self.name}] 筛选出 {len(matched_keywords)} 个相关关键字 {matched_keywords}")

        # Step 3: 获取所有工作流
        _log(f"  [{self.name}] Step 3: 获取所有工作流...")
        if self.all_workflows is None:
            self.all_workflows = self.workflow_skill.get_all_workflows()
            _log(f"  [{self.name}] 从 skill 加载: {len(self.all_workflows)} 个工作流")
        else:
            _log(f"  [{self.name}] 从记忆加载: {len(self.all_workflows)} 个工作流")

        # Step 4: 筛选相关工作流
        _log(f"  [{self.name}] Step 4: 筛选相关工作流...")
        cache_key = f"wf_{query_intent}"
        if cache_key in self._workflow_cache:
            relevant_workflow_names = self._workflow_cache[cache_key]
            _log(f"  [{self.name}] 使用缓存的工作流: {relevant_workflow_names}")
        else:
            relevant_workflow_names = await self._filter_workflows(query_intent, self.all_workflows)
            self._workflow_cache[cache_key] = relevant_workflow_names

        _log(f"  [{self.name}] 筛选出 {len(relevant_workflow_names)} 个相关工作流")

        # Step 5: 合并表
        _log(f"  [{self.name}] Step 5: 合并表...")
        all_tables = []
        table_descriptions = {}
        matched_workflow = None
        table_conditions = {}

        # 从关键字获取表
        tables_from_keywords = self.workflow_skill.get_tables_from_keywords(relevant_keyword_names)
        all_tables.extend(tables_from_keywords)

        # 从工作流获取表
        for wf_name in relevant_workflow_names:
            tables, descs = self.workflow_skill.get_tables_from_workflow(wf_name)
            all_tables.extend(tables)
            table_descriptions.update(descs)
            if not matched_workflow:
                matched_workflow = wf_name
                # 获取 table_conditions
                if wf_name in self.workflow_skill.workflows:
                    wf_data = self.workflow_skill.workflows[wf_name]
                    if isinstance(wf_data, dict):
                        table_conditions = wf_data.get("table_conditions", {})

        # 去重
        all_tables = list(dict.fromkeys(all_tables))
        _log(f"  [{self.name}] 合并后共有 {len(all_tables)} 个表")

        # Step 7: 如果表太多，筛选最多5个
        if len(all_tables) > 5:
            _log(f"  [{self.name}] Step 7: 从 {len(all_tables)} 个表中选出最多5个...")
            all_tables = await self._filter_final_tables(query_intent, all_tables, table_descriptions, matched_keywords, relevant_workflow_names)

        result = {
            "workflow_name": matched_workflow,
            "tables": all_tables,
            "table_descriptions": table_descriptions,
            "needs_clarification": False,
            "clarification_question": "",
            "matched_keywords": matched_keywords,
            "table_conditions": table_conditions,
        }

        return Msg(name=self.name, content=json.dumps(result, ensure_ascii=False), role="assistant")

    async def _filter_keywords(self, query_intent: str, all_keywords: List[str]) -> List[str]:
        """筛选相关关键字"""
        # 构建带描述的关键字列表
        keywords_with_desc = []
        for kw in all_keywords:
            desc = self.workflow_skill.keywords.get(kw, "")
            keywords_with_desc.append(f"- {kw}: {desc}")

        prompt = f"""查询意图: {query_intent}

所有关键字:
{chr(10).join(keywords_with_desc)}
"""

        _log(f"  [{self.name}] [FilterKeywords] Prompt:")
        _log(f"  [{self.name}] [FilterKeywords] User:\n{prompt}")

        messages = [
            {"role": "system", "content": self.keyword_filter_prompt},
            {"role": "user", "content": prompt},
        ]

        content = await _call_model(self.model, messages)
        result = _parse_json_content(content)

        try:
            return result.get("relevant_keywords", [])
        except Exception as e:
            _log(f"  [{self.name}] [FilterKeywords] 解析失败: {e}")
            return []

    async def _filter_workflows(self, query_intent: str, all_workflows: List[Dict[str, str]]) -> List[str]:
        """筛选相关工作流"""
        workflow_list = "\n".join([f"- {wf['name']}: {str(wf['description'])[:200]}" for wf in all_workflows])

        prompt = f"""查询意图: {query_intent}

所有工作流:
{workflow_list}
"""

        _log(f"  [{self.name}] [FilterWorkflows] Prompt:")
        _log(f"  [{self.name}] [FilterWorkflows] User:\n{prompt}")

        messages = [
            {"role": "system", "content": self.workflow_filter_prompt},
            {"role": "user", "content": prompt},
        ]

        content = await _call_model(self.model, messages)
        result = _parse_json_content(content)

        try:
            return result.get("relevant_workflows", [])
        except Exception as e:
            _log(f"  [{self.name}] [FilterWorkflows] 解析失败: {e}")
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

        _log(f"  [{self.name}] [FilterFinalTables] Prompt:")
        _log(f"  [{self.name}] [FilterFinalTables] User:\n{prompt}")
        content = await _call_model(self.model, messages)

        try:
            cleaned = self._clean_json(content)
            result = json.loads(cleaned)
            return result.get("tables", [])
        except Exception as e:
            _log(f"  [{self.name}] [FilterFinalTables] 解析失败: {e}")
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
