#!/usr/bin/env python3
"""
基于 AgentScope 1.0.17 的 SQL 生成多 Agent 系统
使用真实 LLM，支持关联表查询，支持 SQL 执行和结果转换

重构版 - 使用模块化结构
"""
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)
try:
    from cryptography.utils import CryptographyDeprecationWarning
    warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
except ImportError:
    pass

import json
import os
import sys
import asyncio
from typing import Dict, Any, List

from dotenv import load_dotenv
import agentscope
from agentscope.model import OpenAIChatModel

# 确保可以导入包模块
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

try:
    from shop import set_verbose, _patch_dashscope, SQLAgentPipeline
    from shop.utils import _log, _parse_json_content, _call_model
    from shop.skills import WorkflowTableSkill, TableStructureSkill
    from shop.agents import (
        QueryParserAgent,
        WorkflowRetrieverAgent,
        SQLGeneratorAgent,
        SQLValidatorAgent,
        SQLExecutorAgent,
        DataConverterAgent,
        DataTransformAgent,
    )
    from shop.pipeline import PipelineMemory
except ImportError:
    # 降级方案：直接从当前目录导入
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)
    from utils import set_verbose, _patch_dashscope, _log, _parse_json_content, _call_model
    from skills import WorkflowTableSkill, TableStructureSkill
    from agents import (
        QueryParserAgent,
        WorkflowRetrieverAgent,
        SQLGeneratorAgent,
        SQLValidatorAgent,
        SQLExecutorAgent,
        DataConverterAgent,
        DataTransformAgent,
    )
    from pipeline import SQLAgentPipeline, PipelineMemory

# 为了向后兼容，导出所有旧的类和函数
__all__ = [
    'set_verbose',
    '_patch_dashscope',
    'SQLAgentPipeline',
    '_log',
    '_parse_json_content',
    '_call_model',
    'WorkflowTableSkill',
    'TableStructureSkill',
    'QueryParserAgent',
    'WorkflowRetrieverAgent',
    'SQLGeneratorAgent',
    'SQLValidatorAgent',
    'SQLExecutorAgent',
    'DataConverterAgent',
    'DataTransformAgent',
    'PipelineMemory',
]


async def main():
    """主函数"""
    import sys
    # 检查是否有 -v 或 --verbose 参数
    verbose = '-v' in sys.argv or '--verbose' in sys.argv
    set_verbose(True)

    # 检查输出格式
    output_format = "html"
    if '--html' in sys.argv:
        output_format = "html"

    script_dir = os.path.dirname(os.path.abspath(__file__))
    knowledge_dir = os.path.join(script_dir, "knowledge")
    structures_xlsx = os.path.join(knowledge_dir, "table_structures.xlsx")
    output_dir = os.path.join(script_dir, "output")

    load_dotenv()

    # 先修复 dashscope 库（以防万一）
    _patch_dashscope()

    api_key = os.getenv("OPENAI_API_KEY", "dummy")
    model_name = os.getenv("SHOP_OLLAMA_MODEL_NAME", "qwen-max")
    if "qwen3.5" in model_name.lower():
        model_name = "qwen-plus"

    model_kwargs = {
        "model_name": model_name,
        "api_key": api_key,
    }
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

    pipeline = SQLAgentPipeline(knowledge_dir, structures_xlsx, model, output_dir)

    queries = [
        "查询手机号是18610249655的订单"
    ]

    for query in queries:
        result = await pipeline.run(query, output_format=output_format)
        print("\n" + "="*70 + "\n")

        if output_format == "json":
            # JSON 格式直接打印
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        else:
            # HTML 格式打印详细信息
            for task in result.get("tasks"):
                print(f"任务 {task.get('task_id')}: {task.get('original_query', '')}")
                print(f"  成功: {task.get('success', False)}")
                if task.get('success'):
                    print(f"  SQL: {task.get('sql', '')}")
                    print(f"  参数: {task.get('parameters', [])}")
                    conversion_result = task.get('conversion_result', {})
                    print(f"  输出格式: {conversion_result.get('format', '')}")
                    if conversion_result.get('file_path'):
                        print(f"  输出文件: {conversion_result.get('file_path')}")
                else:
                    print(f"  错误: {task.get('error', '')}")


def run():
    """CLI 入口：供 pyproject scripts 调用"""
    asyncio.run(main())


if __name__ == '__main__':
    run()
