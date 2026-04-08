"""
Shop DB AI - 智能数据库查询系统
"""
from .utils import set_verbose, _patch_dashscope
from .pipeline import SQLAgentPipeline

__version__ = "1.0.0"
__all__ = ['set_verbose', '_patch_dashscope', 'SQLAgentPipeline']
