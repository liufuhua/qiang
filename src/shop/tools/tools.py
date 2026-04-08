from langchain_core.documents import Document
import os

def load_table_knowledge(md_path: str = "table_descriptions.md") -> list[Document]:
    """
    读取表结构MD文件，封装为LangChain Document对象（知识库）
    :param md_path: MD文件路径
    :return: 包含表结构信息的Document列表
    """
    # 校验文件是否存在
    if not os.path.exists(md_path):
        raise FileNotFoundError(f"表结构文件 {md_path} 不存在")

    # 读取MD文件内容
    with open(md_path, "r", encoding="utf-8") as f:
        md_content = f.read().strip()

    # 封装为Document（可添加元数据，如文件路径、更新时间）
    table_doc = Document(
        page_content=md_content,
        metadata={
            "source": md_path,
            "type": "table_structure",
            "updated_at": os.path.getmtime(md_path)  # 文件最后修改时间
        }
    )

    # 返回列表（兼容多文档场景）
    return [table_doc]