"""Verification script: checks all modules load and system prompt renders correctly."""
import sys
sys.path.insert(0, '.')

from core.chat_engine import ChatEngine, get_persona
from core.session import ChatSession
from memory.graph.retriever import MemoryRetrieverMiddleware
from memory.graph.writer import MemoryGraphMiddleware

print("=== 模块加载 OK ===")
persona = get_persona()
user_name = persona["user"]["current_name"]
asst_name = persona["assistant"]["current_name"]
print(f"Persona: user={user_name}, assistant={asst_name}")

session = ChatSession(user_id="user_shen")
engine = ChatEngine()
engine.register_middleware(MemoryRetrieverMiddleware())
engine.register_middleware(MemoryExtractionMiddleware())

prompt = engine._build_system_prompt(session)
print()
print("=== System Prompt 预览 (前300字) ===")
print(prompt[:300])
print("...")
print()
print("=== 所有检查通过，可以启动 ===")
