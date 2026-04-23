"""
EbbingFlow — 全局性能与 Token 监测中心
------------------------------------------
支持实时统计每一轮对话的消耗情况，不再逐轮累加显示，确保费用清晰。
"""
import threading
import time
from typing import Dict

class TokenMonitor:
    """单例 Token 监测器 (支持每轮重置统计模式)"""
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        # 统计指标仓库 (用于记录单次任务的活跃消耗)
        self.stats = {
            "chat": {"input": 0, "output": 0, "total": 0},
            "memory": {"input": 0, "output": 0, "total": 0},
            "embedding": {"input": 0, "total": 0}
        }
        # 轮次历史记录 (用于审计)
        self.turn_total = 0

    @classmethod
    def get_instance(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
        return cls._instance

    def reset_for_new_turn(self):
        """每一轮新的对话开始前，重置统计数据以免误导用户"""
        for cat in self.stats:
            for key in self.stats[cat]:
                self.stats[cat][key] = 0
        self.turn_total = 0

    def record_llm_usage(self, category: str, input_tokens: int, output_tokens: int):
        """实时记录 LLM 消耗"""
        if category in self.stats:
            self.stats[category]["input"] += input_tokens
            self.stats[category]["output"] += output_tokens
            self.stats[category]["total"] += (input_tokens + output_tokens)

    def record_embedding_usage(self, text_length: int = 0, tokens: int = 0):
        """记录 Embedding 消耗"""
        actual_tokens = tokens if tokens > 0 else int(text_length * 1.2)
        self.stats["embedding"]["input"] += actual_tokens
        self.stats["embedding"]["total"] += actual_tokens

    def get_report_data(self) -> Dict:
        """返回当前轮次统计数据的深拷贝快照"""
        import copy
        return copy.deepcopy(self.stats)

    def get_report_header(self) -> str:
        """审计报告表头"""
        C_CYAN = "\033[36m"
        C_BOLD = "\033[1m"
        C_RESET = "\033[0m"
        C_DIM = "\033[2m"
        return (
            f"\n{C_BOLD}{C_CYAN}─── 🧠 核心记忆调度与性能审计 (本轮消耗) ───{C_RESET}\n"
            f"{C_DIM}┌──────────────────────────────────────────┬────────────┬────────┬────────┬────────┐{C_RESET}\n"
            f"{C_DIM}│ 调度任务状态 (耗时)                      │ 资源模块   │   In   │   Out  │  Total │{C_RESET}\n"
            f"{C_DIM}├──────────────────────────────────────────┼────────────┼────────┼────────┼────────┤{C_RESET}"
        )

    def get_step_row(self, step_idx: int, message: str, elapsed: float) -> str:
        """单行审计报告 (仅显示当前模块的步进增量)"""
        s = self.stats
        C_GREEN = "\033[32m"
        C_RESET = "\033[0m"
        C_DIM = "\033[2m"
        
        resources = [
            ("👤 对话会话", s['chat']),
            ("🧠 记忆萃取", s['memory']),
            ("🧭 向量嵌入", s['embedding'])
        ]
        label, data = resources[step_idx]
        out_val = str(data.get('output', '-')) if data.get('output', 0) > 0 else "-"
        
        icon = "\u2714" # Checkmark
        return (
            f"{C_DIM}│{C_RESET} {C_GREEN}{icon}{C_RESET} {message:<34} {C_DIM}({elapsed:.1f}s) │{C_RESET} {label:<10} "
            f"{C_DIM}│{C_RESET} {data['input']:>6} {C_DIM}│{C_RESET} {out_val:>6} {C_DIM}│{C_RESET} {data['total']:>6} {C_DIM}│{C_RESET}"
        )

    def get_report_footer(self, total_elapsed: float) -> str:
        """审计报告表尾 (显示本轮总消耗)"""
        s = self.stats
        total = s['chat']['total'] + s['memory']['total'] + s['embedding']['total']
        C_DIM = "\033[2m"
        C_CYAN = "\033[36m"
        C_YELLOW = "\033[33m"
        C_RESET = "\033[0m"
        C_BOLD = "\033[1m"
        
        return (
            f"{C_DIM}├──────────────────────────────────────────┴────────────┴────────┴────────┴────────┤{C_RESET}\n"
            f"{C_DIM}│{C_RESET} {C_BOLD}{C_CYAN}累计用时 {total_elapsed:.1f}s{C_RESET}                      {C_BOLD}{C_YELLOW}📊 本轮预估总消耗{C_RESET}   {C_DIM}│{C_RESET} {C_BOLD}{total:>7}{C_RESET} {C_DIM}│{C_RESET}\n"
            f"{C_DIM}└─────────────────────────────────────────────────────────────────────────────────┘{C_RESET}\n"
            f"{C_DIM}---------- 💾 数据库同步完毕，所有思维已落盘 ----------{C_RESET}\n"
        )

# 全局单例
token_monitor = TokenMonitor.get_instance()
