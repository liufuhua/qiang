from typing import Dict, List, Tuple, Any
import os
import re
import json
from pyMysqlClient import get_mysql_client  # 引用已封装的数据库实例
import openpyxl
import ast
from table_config import BLACK_TABLES

# ------------------------------
# 核心配置
# ------------------------------
# MD文件输出路径（可自定义）
TABLE_PATH = "knowledge/table_descriptions.xlsx"
STRUCT_PATH = "knowledge/table_structures.xlsx"
KEYWORDS_TABLE_PATH = "knowledge/keywords_table.json"

cursor = get_mysql_client()


# ------------------------------
# 工具函数：获取数据库元数据
# ------------------------------
def get_all_table_names() -> List[str]:
    """
    获取数据库中所有非系统表名
    """
    all_tables = cursor.run_sql("show tables")
    # 过滤忽略的表
    valid_tables = []
    for table in all_tables:
        if table[0] not in BLACK_TABLES:
            valid_tables.append(table[0])
    return valid_tables


def get_table_comment(table_name: str) -> str:
    """
    获取表的注释（表描述）
    """
    # 执行SQL查询表注释
    try:
        # MySQL查询表注释的SQL
        sql = f"""
        SELECT TABLE_COMMENT
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table_name}'
        """
        # 通过db_instance执行原生SQL
        result = cursor.run_sql(sql)
        b = result[0]
        # 清理结果（去除换行/空格）
        comment = re.sub(r'\s+', ' ', b[0].strip()) if result else f"{table_name}表（无注释）"
        return comment
    except Exception as e:
        return f"{table_name}表（获取注释失败：{str(e)[:20]}）"


def get_table_structure(table_name: str) -> list[list[str | Any]]:
    """
    获取表的字段结构和字段注释
    返回：(字段结构列表, 字段注释字典)
    """
    # 获取表结构（langchain封装的格式）
    tableInfo = cursor.run_sql(f"desc {table_name}")
    commonInfo = cursor.run_sql(f"""
                SELECT COLUMN_NAME, COLUMN_COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table_name}'
    """)
    commonMap = {}
    for common in commonInfo:
        commonMap[common[0]] = common[1]
    result = []
    for item in tableInfo:
        result.append([item[0], item[1], "Y" if item[2] == "YES" else "N", "Y" if item[3] else "N", commonMap[item[0]]])

    return result


# ------------------------------
# 生成excel文件
# ------------------------------
def generate_table_descriptions(tables: List[str]):
    """
    生成 keywords_table.json，格式是 表备注:表名
    """
    result = {}
    for table in tables:
        comment = get_table_comment(table)
        if comment:
            result[comment] = table
        else:
            print("❌ ", table)

    # 写入JSON文件
    if os.path.exists(KEYWORDS_TABLE_PATH):
        os.remove(KEYWORDS_TABLE_PATH)

    with open(KEYWORDS_TABLE_PATH, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"✅ 表描述JSON文件已生成：{os.path.abspath(KEYWORDS_TABLE_PATH)}")


def generate_table_structures(tables: List[str]):
    """
    生成表结构Excel文件
    列：表名, 字段, 类型, 可空, 索引, 列名, 转义, 说明, 关联
    """
    if os.path.exists(STRUCT_PATH):
        os.remove(STRUCT_PATH)

    workBook = openpyxl.Workbook(write_only=True)
    sheet = workBook.create_sheet("tableStructures")
    sheet.append(["表名", "字段", "类型", "可空", "索引", "列名", "转义", "说明", "关联"])

    def simplify_type(db_type: str) -> str:
        """简化类型为数字/文本/时间"""
        db_type = db_type.lower()
        if any(t in db_type for t in ['int', 'decimal', 'float', 'double', 'numeric', 'bigint', 'tinyint', 'smallint']):
            return '数字'
        elif any(t in db_type for t in ['datetime', 'timestamp', 'date', 'time']):
            return '时间'
        else:
            return '文本'

    def simplify_boolean(value: str) -> str:
        """简化为是/否"""
        return '是' if value in ['Y', 'YES', True, 'y', 'yes'] else '否'

    table_names_set = set(tables)

    for idx, table in enumerate(tables):
        # 获取表结构
        structure = get_table_structure(table)

        # 表结构
        for s in structure:
            remark = s[4]

            # 如果备注包括"无效"，则跳过这个字段
            if remark and "无效" in remark:
                continue

            simple_type = simplify_type(s[1])
            simple_nullable = simplify_boolean(s[2])
            simple_index = simplify_boolean(s[3])

            # 解析备注
            col_name = ""
            escape = ""
            desc = ""
            relation = ""

            if remark:
                # 使用"/"拆分
                parts = [p.strip() for p in remark.split("/") if p.strip()]

                if len(parts) == 1:
                    # 如果只有一条数据，则只是列名
                    col_name = parts[0]
                elif len(parts) > 1:
                    # 第一条是列名
                    col_name = parts[0]

                    # 解析剩余部分
                    for part in parts[1:]:
                        if "=" in part:
                            # 如果数据包含"="则是转义
                            escape = part
                        elif any(tbl in part for tbl in table_names_set):
                            # 如果数据包含某个表名则是关联
                            relation = part
                        else:
                            # 不然是说明
                            desc = part

            sheet.append([table, s[0], simple_type, simple_nullable, simple_index, col_name, escape, desc, relation])

    # 写入文件
    workBook.save(STRUCT_PATH)
    print(f"✅ 表结构文件已生成：{os.path.abspath(STRUCT_PATH)}")


# ------------------------------
# 主函数
# ------------------------------
def main():
    print("🔍 开始读取数据库元数据...")

    # 1. 获取所有有效表名
    tables = get_all_table_names()
    if not tables:
        print("❌ 未找到有效表（已过滤系统表/临时表）")
        return
    print(f"📊 共检测到 {len(tables)} 张有效表：{', '.join(tables)}")

    # 2. 生成表描述 (keywords_table.json)
    generate_table_descriptions(tables)

    # 3. 生成表结构
    generate_table_structures(tables)

    print("\n🎉 所有文件生成完成！")


if __name__ == "__main__":
    r = input("重新生成会覆盖当前文件,请输入yes确认: ")
    if r == 'yes':
        main()
    else:
        print("已取消")
