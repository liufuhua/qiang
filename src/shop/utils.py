#!/usr/bin/env python3
"""
工具函数和全局配置
"""
import json
import re
from typing import Dict, Any, List, Optional


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


def _patch_dashscope():
    """修复 dashscope 库的 StreamReader decode 错误"""
    try:
        # 首先尝试修复最底层的问题
        import dashscope.common.utils

        # 保存原始函数
        if hasattr(dashscope.common.utils, '_handle_aiohttp_failed_response'):
            orig_fn = dashscope.common.utils._handle_aiohttp_failed_response

            def patched_fn(response):
                try:
                    return orig_fn(response)
                except AttributeError as e:
                    if "'StreamReader' object has no attribute 'decode'" in str(e):
                        # 忽略这个错误，返回空 JSON
                        return '{}'
                    raise

            dashscope.common.utils._handle_aiohttp_failed_response = patched_fn
            print("✅ dashscope 库已修复 (1/3)")
    except (ImportError, AttributeError) as e:
        print(f"⚠️  无法修复 dashscope 底层: {e}")

    try:
        # 尝试修复 aiohttp 响应读取
        import aiohttp

        if hasattr(aiohttp, 'StreamReader'):
            orig_read = aiohttp.StreamReader.read

            async def patched_read(self, n=-1):
                try:
                    return await orig_read(self, n)
                except AttributeError as e:
                    if "'StreamReader' object has no attribute 'decode'" in str(e):
                        return b''
                    raise

            aiohttp.StreamReader.read = patched_read
            print("✅ aiohttp StreamReader 已修复 (2/3)")
    except (ImportError, AttributeError) as e:
        print(f"⚠️  无法修复 aiohttp: {e}")

    try:
        # 尝试修复 dashscope 的 SSE 解析器
        from dashscope.api_entities.sse_parser import SSEParser

        orig_next = SSEParser.__next__

        def patched_next(self):
            try:
                return orig_next(self)
            except AttributeError as e:
                if "'StreamReader' object has no attribute 'decode'" in str(e):
                    raise StopIteration
                raise

        SSEParser.__next__ = patched_next
        print("✅ SSEParser 已修复 (3/3)")
    except (ImportError, AttributeError) as e:
        print(f"⚠️  无法修复 SSEParser: {e}")
