#!/usr/bin/env python3
"""
基于 FastAPI 的问答页面
"""
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)
try:
    from cryptography.utils import CryptographyDeprecationWarning
    warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
except ImportError:
    pass

import os
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import uvicorn

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from dotenv import load_dotenv
load_dotenv()

# 从包导入
from shop import set_verbose, _patch_dashscope, SQLAgentPipeline


# 全局 pipeline 实例
pipeline = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用启动和关闭时的生命周期管理"""
    global pipeline

    # 设置 verbose 模式
    set_verbose(True)

    # 初始化 pipeline
    script_dir = os.path.dirname(os.path.abspath(__file__))
    knowledge_dir = os.path.join(script_dir, "knowledge")
    structures_xlsx = os.path.join(knowledge_dir, "table_structures.xlsx")
    output_dir = os.path.join(script_dir, "output")

    # 先修复 dashscope 库（以防万一）
    _patch_dashscope()

    from agentscope.model import OpenAIChatModel

    api_key = os.getenv("OPENAI_API_KEY", "dummy")
    model_name = os.getenv("SHOP_OLLAMA_MODEL_NAME", "qwen-max")

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

    # 检查并修改 _model 属性
    if hasattr(model, '_model'):
        inner_model = model._model
        if hasattr(inner_model, 'generate_args'):
            inner_model.generate_args['stream'] = False
        if hasattr(inner_model, '_default_generate_args'):
            inner_model._default_generate_args['stream'] = False

    pipeline = SQLAgentPipeline(knowledge_dir, structures_xlsx, model, output_dir)
    print("✅ Pipeline 初始化完成")

    yield

    # 清理
    pipeline = None


app = FastAPI(lifespan=lifespan)

# 配置模板
templates_dir = os.path.join(script_dir, "templates")
os.makedirs(templates_dir, exist_ok=True)
templates = Jinja2Templates(directory=templates_dir)

# 配置静态文件
static_dir = os.path.join(script_dir, "static")
os.makedirs(static_dir, exist_ok=True)
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

# 配置 output 目录
output_dir = os.path.join(script_dir, "output")
os.makedirs(output_dir, exist_ok=True)
if os.path.exists(output_dir):
    app.mount("/output", StaticFiles(directory=output_dir), name="output")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """主页"""
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/ask")
async def ask(request: Request):
    """处理用户问题"""
    data = await request.json()
    question = data.get("question", "")

    if not question:
        return {
            "needs_database": False,
            "friendly_reply": "请输入您的问题",
            "tasks": [],
        }

    # 使用 pipeline 处理问题，使用 HTML 格式
    result = await pipeline.run(question, output_format="html")

    # 处理 HTML 内容，提取到 conversion_result.content 中，并删除本地文件
    for task in result.get("tasks", []):
        if task.get("success"):
            conversion = task.get("conversion_result", {})
            if conversion and conversion.get("format") == "html" and conversion.get("file_path"):
                file_path = conversion["file_path"]
                if os.path.exists(file_path):
                    try:
                        # 读取 HTML 文件内容
                        with open(file_path, "r", encoding="utf-8") as f:
                            html_content = f.read()
                        conversion["content"] = html_content
                        # 删除本地文件
                        os.remove(file_path)
                        conversion["file_path"] = None
                    except Exception as e:
                        print(f"Warning: Could not read/delete HTML file: {e}")

    return result


if __name__ == "__main__":
    uvicorn.run(
        "webapp:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )


"""
    关键字:
        表描述: 表名
        关键字: 关键字描述
    只传关键字, 通过查询, 找出 描述
    
    筛选工作流
    
    通过关键字结果, 工作流结果 得到合并后的表
    查询表结构, 关键字结果, 工作流结果, 生成sql 
    
    字段转义
"""