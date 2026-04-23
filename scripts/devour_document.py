"""
长文档吞噬测试工具 (Document Devourer Test)
用于验证“文档吞噬器”：滑动窗口切片 -> 批量向量化 -> 事件提取。
"""
import sys
import os
import asyncio
from unittest.mock import MagicMock

# 强制禁止本地请求走代理
os.environ["NO_PROXY"] = "127.0.0.1,localhost"

# 确保引入当前项目根目录
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from memory.vector.devourer import DocumentDevourer
from memory.identity.resolver import Actor

C_GREEN = "\033[32m"
C_RED = "\033[31m"
C_CYAN = "\033[36m"
C_YELLOW = "\033[33m"
C_RESET = "\033[0m"

# 一段测试用的长文本（模拟商业报告/短篇故事）
TEST_DOCUMENT = """
2025年度星巴克亚太区拓展计划纪要：
今年，星巴克计划在亚太地区新增 500 家门店。主要负责人是李明，他之前是瑞幸的区域经理。
李明在昨天的会议上提出，要在上海徐汇区开设一家全自动化的概念店。
资金方面，概念店预算高达 500 万人民币。
然而，设计总监张华对这个预算表示担忧，张华认为自动化设备的维护成本太高。
最终，CEO 王总拍板决定，先在徐汇区试水，如果盈利则向全国推广。
这个决定让李明非常高兴，他打算下周就去上海实地考察。
同时，张华也被安排去考察自动化设备的供应商。
这是一场充满挑战的拓展，所有人都在密切关注。
"""

async def main():
    print(f"\n{C_CYAN}{'='*60}")
    print(" [TEST] 文档吞噬器 (Document Devourer)")
    print(f"{'='*60}{C_RESET}\n")

    devourer = DocumentDevourer(chunk_size=150, overlap=30)
    
    # 模拟一个执行文档吞噬的主体（比如用户）
    actor = Actor(
        speaker_id="user_shen",
        speaker_name="神",
        target_id="system",
        target_name="AI"
    )

    try:
        # 这里会将长文切片、存入 ChromaDB，并萃取关系入图谱
        result = await devourer.devour(
            text=TEST_DOCUMENT,
            source_name="2025年度星巴克战略会议纪要",
            actor=actor,
            session_id="doc_test_001"
        )
        
        print(f"\n{C_GREEN}文档吞噬成功！{C_RESET}")
        print(f"数据源: {result['source']}")
        print(f"生成的向量切片数: {result['chunks_stored']}")
        print(f"提取出的结构化事件数: {result['events_extracted']}\n")
    except Exception as e:
        print(f"\n{C_RED}文档吞噬失败: {e}{C_RESET}")

if __name__ == "__main__":
    asyncio.run(main())
