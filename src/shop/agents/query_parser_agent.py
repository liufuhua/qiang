#!/usr/bin/env python3
"""
Query Parser Agent - 问题解析与分类
"""
import json
import re
from typing import Dict, Any

from agentscope.agent import AgentBase
from agentscope.message import Msg
from agentscope.model import ChatModelBase

from ..utils import _log, _call_model


class QueryParserAgent(AgentBase):
    """问题解析与分类 Agent"""

    def __init__(self, model: ChatModelBase, name: str = "QueryParser"):
        super().__init__()
        self.name = name
        self.model = model
        self.sys_prompt = """你是一个数据库查询问题解析专家。

任务说明：
1. 判断用户的问题是否需要通过查询数据库来解决。
2. 如果需要查询，进一步分析用户的问题是否包含了多个独立的查询请求。
3. 对于包含多个查询请求的情况，将每个查询拆分为独立的任务。
4. 对于每一个独立的任务，明确其查询意图，并从用户提供的信息中提取出关键参数。注意，关键参数必须直接从用户的问题中提取，不得自行编造。
5. 如果判断结果为不需要进行数据库查询，则给出一个友好的回复。

输出格式要求：请按照以下JSON格式组织你的答案：

```json
{
  "needs_database": ${true/false}, // 标记是否需要访问数据库
  "friendly_reply": "${友好回复}", // 当不需要查询数据库时提供（仅当needs_database=false时填写）
  "tasks": [ // 需要执行的具体查询任务列表
    {
      "task_id": ${任务编号}, // 每个任务的唯一标识符
      "query_intent": "${用户想查询什么}", // 描述该查询的目的或意图
      "key_parameters": ["${参数1}", "${参数2}", ...], // 从用户问题中提取的关键参数列表
      "original_query": "${对应的原始查询语句片段}" // 用户提出的原始查询内容
    },
    ...
  ]
}
```

示例1：如果用户询问"你好"
应返回:
```json
{
  "needs_database": false,
  "friendly_reply": "你好！有什么可以帮助您的吗？",
  "tasks": []
}
```

示例2：如果用户询问"查询2026年1月的订单"
应返回:
```json
{
  "needs_database": true,
  "friendly_reply": "",
  "tasks": [
    {
      "task_id": 1,
      "query_intent": "查询指定月份的所有订单记录",
      "key_parameters": ["2026年1月"],
      "original_query": "查询2026年1月的订单"
    }
  ]
}
```

注意事项：
- 确保每个`task`代表一个独立且具体的查询需求。
- 使用逗号、分号或句号等标点符号正确地分割不同的查询请求。
- `key_parameters`字段中的值必须完全基于用户的提问内容提取，不允许添加任何未提及的信息。
- 若用户问题中没有明确指出具体参数，则`key_parameters`可以为空数组`[]`。
"""

    async def __call__(self, msg: Msg = None) -> Msg:
        user_query = msg.content if msg else ""

        messages = [
            {"role": "system", "content": self.sys_prompt},
            {"role": "user", "content": f"用户问题: {user_query}"},
        ]

        _log(f"  [{self.name}] 调用 LLM...")
        _log(f"  [{self.name}] Prompt:")
        _log(f"  [{self.name}] System: {self.sys_prompt[:150]}...")
        _log(f"  [{self.name}] User: 用户问题: {user_query}")
        content = await _call_model(self.model, messages)
        _log(f"  [{self.name}] LLM 响应: {content[:300]}...")

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

            _log(f"  [{self.name}] 解析成功: 共 {len(result.get('tasks', []))} 个任务")
            for i, task in enumerate(result.get("tasks", [])):
                _log(f"    任务 {i+1}: {task.get('query_intent', '')}")
        except Exception as e:
            _log(f"  [{self.name}] 解析失败: {e}")
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
