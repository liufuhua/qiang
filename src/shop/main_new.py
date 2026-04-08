#!/usr/bin/env python3
"""
基于 AgentScope 1.0.17 的 SQL 生成多 Agent 系统
使用真实 LLM，支持关联表查询，支持 SQL 执行和结果转换

重构版 - 使用模块化结构
"""
import json
import os
import asyncio
from typing import Dict, Any, List

from dotenv import load_dotenv
import agentscope
from agentscope.model import OpenAIChatModel

from . import set_verbose, _patch_dashscope, SQLAgentPipeline
from .utils import _log


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
        "查询手机号是18610249655的会员信息"
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


if __name__ == '__main__':
    import pymysql
    asyncio.run(main())
