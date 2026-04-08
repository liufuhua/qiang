#!/usr/bin/env python3
"""
SQL 生成 Agent
"""
import json
import re
from datetime import datetime
from typing import Dict, Any, List

from agentscope.agent import AgentBase
from agentscope.message import Msg
from agentscope.model import ChatModelBase

from ..utils import _log, _parse_json_content, _call_model
from ..skills import WorkflowTableSkill, TableStructureSkill


class SQLGeneratorAgent(AgentBase):
    """SQL 生成 Agent"""

    def __init__(
        self,
        knowledge_dir: str,
        structures_xlsx_path: str,
        model: ChatModelBase,
        name: str = "SQLGenerator",
    ):
        super().__init__()
        self.name = name
        self.model = model
        self.table_skill = TableStructureSkill(structures_xlsx_path)
        self.workflow_skill = WorkflowTableSkill(knowledge_dir)
        self.sys_prompt = """你是一个 MySQL 5.7 SQL 生成专家。

根据用户的查询意图、关键参数和表结构，生成安全的只读 SQL。

表结构字段说明：
- 列名: 查询结果显示的列名，使用 AS 将字段别名为列名
- 转义: 查询时需要转义的信息（如枚举值映射）
- 说明: 查询时需要注意的信息

【非常重要】"查询注意事项"是表查询时必须遵守的规则，优先级最高！
- 仔细阅读每个表的"查询注意事项"
- 严格按照注意事项中的要求生成 SQL
- 注意事项中提到的索引、计算方式、条件等必须遵守

要求：
1. 只生成 SELECT语句
2. 使用 MySQL 5.7 语法
3. 只在必要时进行多表关联查询（JOIN），尽量减少 JOIN
4. 只使用提供的表结构中存在的表和字段！
5. 查询字段要精简，只返回查询意图相关的字段，不要返回所有字段
6. 如果字段有"列名"，使用 AS 将字段别名设为列名
7. 注意"转义"信息，在查询时按转义规则处理
8. 仔细阅读"说明"信息，查询时需要注意这些要点
9. 【最高优先级】严格遵守每个表的"查询注意事项"！
10. 返回格式为 JSON：
   {
     "sql": "生成的 SQL 语句，使用 %s 作为参数占位符",
     "parameters": ["参数1", "参数2", ...]
   }

【重要】最小查询原则：
- 如果用户没有指定要查询哪些具体字段，只返回最核心的字段（如 id、name 等）
- 只在确实需要关联表的数据时才使用 JOIN，不要为了关联而关联
- 不要返回不必要的字段（如 created_at、updated_at、password 等，除非用户明确要求）


"""

    async def __call__(self, msg: Msg = None) -> Msg:
        content = msg.content if msg else "{}"
        input_data = _parse_json_content(content)
        query_intent = input_data.get("query_intent", "")
        key_parameters = input_data.get("key_parameters", [])
        tables = input_data.get("tables", [])
        table_descriptions = input_data.get("table_descriptions", {})
        matched_keywords = input_data.get("matched_keywords", [])
        workflow_name = input_data.get("workflow_name", "")
        table_conditions = input_data.get("table_conditions", {})
        regenerate_hint = input_data.get("regenerate_hint", "")

        # Step 1: 获取表结构和表备注
        _log(f"  [{self.name}] 获取表结构...")
        table_schemas = self.table_skill.get_all_table_schemas(tables)
        table_remarks = self.table_skill.get_all_table_remarks(tables)
        missing_tables = self.table_skill.get_missing_tables(tables)
        _log(f"  [{self.name}] 缺失表: {missing_tables}")
        _log(f"  [{self.name}] 表备注: {table_remarks}")

        # 获取当前时间
        now = datetime.now()
        current_time_str = now.strftime("%Y-%m-%d %H:%M:%S")
        current_date_str = now.strftime("%Y-%m-%d")
        _log(f"  [{self.name}] 当前时间: {current_time_str}")

        # 如果有缺失表，反馈给第二步
        if missing_tables:
            _log(f"  [{self.name}] 有缺失表，需要补充...")
            # 这里可以请求第二步补充表，但先继续使用现有表

        # Step 2: 构建提示
        schema_info = []
        for table_name in tables:
            if table_name in table_schemas:
                desc = table_descriptions.get(table_name, "")
                remark = table_remarks.get(table_name, "")
                table_info = f"表: {table_name}\n说明: {desc}"
                if remark:
                    table_info += f"\n查询注意事项: {remark}"
                table_info += f"\n结构:\n{table_schemas[table_name]}"
                schema_info.append(table_info)
            elif table_name in table_descriptions:
                remark = table_remarks.get(table_name, "")
                table_info = f"表: {table_name}\n说明: {table_descriptions[table_name]}"
                if remark:
                    table_info += f"\n查询注意事项: {remark}"
                table_info += "\n结构: 未知"
                schema_info.append(table_info)

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
            table_relation_info += "\n【重要】这里描述了表之间的关联关系，生成 JOIN 时请需要仔细阅读："
            for t_name, t_cond in table_conditions.items():
                table_relation_info += f"\n  - {t_name}: {t_cond}"

        prompt = f"""查询意图: {query_intent}
关键参数: {', '.join(key_parameters) if key_parameters else '无'}{keyword_info}{workflow_info}{table_relation_info}

当前时间: {current_time_str}
当前日期: {current_date_str}

表信息:
{chr(10).join(schema_info)}

{f'重新生成提示: {regenerate_hint}' if regenerate_hint else ''}

【重要提示 · 必须严格遵守】
1. 【最高优先级】必须严格遵守每个表的“查询注意事项”，这是强制性规则。

2. 最小查询原则：只返回与查询意图相关的字段，禁止使用 SELECT *。

3. 若字段配置了“列名”，必须使用 AS 设置中文别名。
   示例：SELECT name AS 姓名, sex AS 性别

4. 若字段存在“转义规则”，必须使用 CASE WHEN 转换为中文含义。
   示例：CASE status WHEN 0 THEN '否' ELSE '是' END AS 状态

5. 必须仔细阅读表结构中的“说明”，并按说明要求查询。

6. 尽量避免多表关联，仅在必要时使用 JOIN。
   关联必须按照“表关系说明”执行。

7. 关联查询必须使用主键/外键，禁止编造关联关系。

8. 仅使用表结构中存在的字段，禁止编造不存在的表或字段。
   若缺少必要表，必须明确提出。

9. 日期条件必须使用标准函数：
   - 今天：current_date
   - 当月：date_format(时间字段, '%Y-%m') = date_format(current_date, '%Y-%m')
   - 今年：date_format(时间字段, '%Y') = date_format(current_date, '%Y')

10. 所有查询必须分页：
    每页最多 20 条，最多查询第 10 页。


【主表选择规则 · 最高优先级】
1. 优先选择 **带有 WHERE 精准过滤条件** 的表作为主表（FROM 后第一个表）。
2. WHERE 条件作用在哪个表，哪个表就是主表。
3. 必须先过滤主表，再关联其他表。
4. 禁止忽略、遗漏任何 WHERE 条件。
5. 保证查询性能最优、结果准确、不丢失数据。


【注意事项 · 再次强调】
- 严格按表结构提供的字段查询，不臆造字段与表。
- 关联必须使用正确的主键/外键。
- 日期格式必须正确。
- 再次强调：必须严格遵守表中的“查询注意事项”。

{f'- 请特别注意: {regenerate_hint}' if regenerate_hint else ''}
"""

        messages = [
            {"role": "system", "content": self.sys_prompt},
            {"role": "user", "content": prompt},
        ]

        _log(f"  [{self.name}] 调用 LLM...")
        _log(f"  [{self.name}] Prompt:")
        _log(f"  [{self.name}] System: {self.sys_prompt[:100]}...")
        _log(f"  [{self.name}] User:\n{prompt}")
        content = await _call_model(self.model, messages)
        _log(f"  [{self.name}] LLM 响应: {content[:200]}...")

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
            _log(f"  [{self.name}] 解析成功")
        except Exception as e:
            _log(f"  [{self.name}] 解析失败: {e}")
            result = {"sql": "", "parameters": key_parameters if key_parameters else []}

        result["missing_tables"] = missing_tables
        return Msg(name=self.name, content=json.dumps(result, ensure_ascii=False), role="assistant")
