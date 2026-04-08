#!/usr/bin/env python3
"""
Skills package
"""
try:
    from .workflow_table_skill import WorkflowTableSkill
    from .table_structure_skill import TableStructureSkill
except ImportError:
    from workflow_table_skill import WorkflowTableSkill
    from table_structure_skill import TableStructureSkill

__all__ = ['WorkflowTableSkill', 'TableStructureSkill']
