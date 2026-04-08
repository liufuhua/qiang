#!/usr/bin/env python3
"""
SQL Agent Pipeline - 主协调器
"""
import json
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

import agentscope
from agentscope.agent import AgentBase
from agentscope.message import Msg
from agentscope.model import ChatModelBase

from .utils import _log, _parse_json_content, _call_model
from .skills import WorkflowTableSkill, TableStructureSkill
from .agents import (
    QueryParserAgent,
    WorkflowRetrieverAgent,
    SQLGeneratorAgent,
    SQLValidatorAgent,
    SQLExecutorAgent,
    DataConverterAgent,
)


@dataclass
class PipelineMemory:
    """Pipeline 记忆/缓存"""
    all_keywords: Optional[List[Dict[str, str]]] = None
    all_workflows: Optional[List[Dict[str, str]]] = None


class SQLAgentPipeline:
    """SQL 生成协调器"""

    def __init__(
        self,
        knowledge_dir: str,
        structures_xlsx_path: str,
        model: ChatModelBase,
        output_dir: str = None,
    ):
        self.workflow_skill = WorkflowTableSkill(knowledge_dir)
        self.table_skill = TableStructureSkill(structures_xlsx_path)

        self.query_parser = QueryParserAgent(model)
        self.workflow_retriever = WorkflowRetrieverAgent(model, self.workflow_skill)
        self.sql_generator = SQLGeneratorAgent(knowledge_dir, structures_xlsx_path, model)
        self.sql_validator = SQLValidatorAgent(self.table_skill)
        self.sql_executor = SQLExecutorAgent()
        self.data_converter = DataConverterAgent(self.table_skill, output_dir)

        # 预加载知识库到记忆
        self.memory = PipelineMemory()
        _log("[Pipeline] 预加载知识库...")
        self.memory.all_keywords = self.workflow_skill.get_all_keywords()
        self.memory.all_workflows = self.workflow_skill.get_all_workflows()
        self.workflow_retriever.all_keywords = self.memory.all_keywords
        self.workflow_retriever.all_workflows = self.memory.all_workflows
        _log(f"[Pipeline] 预加载完成: {len(self.memory.all_keywords)} 个关键字, {len(self.memory.all_workflows)} 个工作流")

    async def _run_single_task(
        self,
        task: Dict[str, Any],
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        """运行单个任务"""
        task_id = task.get("task_id", 1)
        original_query = task.get("original_query", "")
        query_intent = task.get("query_intent", "")
        key_parameters = task.get("key_parameters", [])

        _log(f"\n{'='*70}")
        _log(f"[任务 {task_id}] {original_query}")
        _log(f"{'='*70}")

        # Step 1: 检索工作流和表
        _log(f"\n[任务 {task_id}] Step 1: 检索工作流和表...")
        retriever_input = {
            "query_intent": query_intent,
            "key_parameters": key_parameters,
        }
        retriever_msg = await self.workflow_retriever(
            Msg(name="QueryParser", content=json.dumps(retriever_input, ensure_ascii=False), role="user")
        )
        workflow_result = _parse_json_content(retriever_msg.content)

        if workflow_result.get("needs_clarification", False):
            _log(f"  需要澄清: {workflow_result.get('clarification_question', '')}")
            return {
                "task_id": task_id,
                "original_query": original_query,
                "success": False,
                "needs_clarification": True,
                "clarification_question": workflow_result.get("clarification_question", ""),
            }

        _log(f"  工作流: {workflow_result.get('workflow_name', '无')}")
        tables = workflow_result.get("tables", [])
        _log(f"  相关表: {', '.join(tables) if tables else '无'}")

        # Step 2 & 3 & 4: SQL 生成、验证、执行（带重试）
        _log(f"\n[任务 {task_id}] Step 2 & 3 & 4: 生成、验证并执行 SQL...")

        generator_input = {
            "query_intent": query_intent,
            "key_parameters": key_parameters,
            **workflow_result,
        }

        last_error = ""
        for attempt in range(max_retries):
            _log(f"  尝试 {attempt + 1}/{max_retries}...")

            # 生成 SQL
            generator_msg = await self.sql_generator(
                Msg(name="WorkflowRetriever", content=json.dumps(generator_input, ensure_ascii=False), role="user")
            )
            sql_result = _parse_json_content(generator_msg.content)

            # 验证 SQL
            validator_msg = await self.sql_validator(
                Msg(name="SQLGenerator", content=json.dumps(sql_result, ensure_ascii=False, default=str), role="user")
            )
            validation_result = _parse_json_content(validator_msg.content)

            if not validation_result.get("is_valid", False):
                _log(f"  ✗ {validation_result.get('validation_message', '')}")
                if validation_result.get("needs_regenerate", False) and attempt < max_retries - 1:
                    _log(f"  重新生成...")
                    generator_input["regenerate_hint"] = validation_result.get("regenerate_hint", "")
                    last_error = validation_result.get("validation_message", "")
                    continue
                else:
                    last_error = validation_result.get("validation_message", "")
                    break

            _log(f"  ✓ {validation_result.get('validation_message', '')}")

            # 执行 SQL
            sql = validation_result.get("sql", "")
            parameters = validation_result.get("parameters", [])

            executor_input = {
                "sql": sql,
                "parameters": parameters,
            }
            executor_msg = await self.sql_executor(
                Msg(name="SQLValidator", content=json.dumps(executor_input, ensure_ascii=False, default=str), role="user")
            )
            execution_result = _parse_json_content(executor_msg.content)

            if execution_result.get("success", False):
                _log(f"  ✓ SQL 执行成功")
                _log(f"\n  任务 {task_id} 结果:")
                _log(f"  SQL: {sql}")
                _log(f"  参数: {parameters}")

                return {
                    "task_id": task_id,
                    "original_query": original_query,
                    "success": True,
                    "query_intent": query_intent,
                    "key_parameters": key_parameters,
                    "workflow_result": workflow_result,
                    "sql": sql,
                    "parameters": parameters,
                    "execution_result": execution_result,
                }
            else:
                _log(f"  ✗ SQL 执行失败: {execution_result.get('error_message', '')}")
                if execution_result.get("needs_retry", False) and attempt < max_retries - 1:
                    _log(f"  重新生成 SQL...")
                    generator_input["regenerate_hint"] = f"执行错误: {execution_result.get('error_message', '')}"
                    last_error = execution_result.get("error_message", "")

                    # 尝试补充关联表
                    sql_used_tables = self.sql_validator._extract_tables(sql)
                    _log(f"  [Pipeline] SQL 中使用的表: {sql_used_tables}")
                    new_tables = list(workflow_result.get("tables", []))
                    for table_name in sql_used_tables:
                        related_tables = self.table_skill.find_related_tables(table_name)
                        _log(f"  [Pipeline] 找到关联表: {related_tables}")
                        for rt in related_tables:
                            if rt not in new_tables:
                                new_tables.append(rt)
                    if new_tables != workflow_result.get("tables", []):
                        _log(f"  [Pipeline] 补充表后: {new_tables}")
                        workflow_result["tables"] = new_tables
                        generator_input = {
                            "query_intent": query_intent,
                            "key_parameters": key_parameters,
                            **workflow_result,
                        }
                    continue
                else:
                    last_error = execution_result.get("error_message", "")
                    break

        _log(f"\n任务 {task_id} 未能成功执行")
        return {
            "task_id": task_id,
            "original_query": original_query,
            "success": False,
            "query_intent": query_intent,
            "key_parameters": key_parameters,
            "workflow_result": workflow_result,
            "error": last_error or "无法生成有效的 SQL",
        }

    async def run(
        self,
        user_query: str,
        max_retries: int = 3,
        output_format: str = "json",
    ) -> Dict[str, Any]:
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

        # 对每个成功的任务进行数据转换
        final_results = []
        for task_result in task_results:
            if task_result.get("success", False):
                execution_result = task_result.get("execution_result", {})
                workflow_result = task_result.get("workflow_result", {})

                # 直接使用原始数据
                raw_data = execution_result.get("data", [])

                # 从原始数据中提取所有列
                all_columns = []
                if raw_data:
                    # 收集第一行的所有键作为列
                    all_columns = list(raw_data[0].keys())

                # 数据转换（JSON/HTML 格式）
                converter_input = {
                    "data": raw_data,
                    "columns": all_columns,
                    "format": output_format,
                    "task_id": task_result.get("task_id", 1),
                    "tables": workflow_result.get("tables", []),
                }
                converter_msg = await self.data_converter(
                    Msg(name="SQLExecutor", content=json.dumps(converter_input, ensure_ascii=False, default=str), role="user")
                )
                conversion_result = _parse_json_content(converter_msg.content)
                task_result["conversion_result"] = conversion_result

            final_results.append(task_result)

        # 汇总结果
        _log(f"\n{'='*70}")
        _log("所有任务执行完成")
        _log(f"{'='*70}")
        for i, result in enumerate(final_results):
            status = "✓" if result.get("success", False) else "✗"
            _log(f"{status} 任务 {result.get('task_id', i+1)}: {result.get('original_query', '')}")

        return {
            "needs_database": True,
            "original_query": user_query,
            "task_count": len(final_results),
            "tasks": final_results,
            "output_format": output_format,
        }
