"""
EbbingFlow 全局配置模块
从 .env 文件读取所有配置，提供统一的配置访问接口
"""
import os
from dotenv import load_dotenv

load_dotenv(override=True)
print("[Config] Environment variables loaded (override=True)")

# ================= 全局静音配置 =================
os.environ["TOKENIZERS_PARALLELISM"] = "false"
os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
# os.environ["HF_HUB_OFFLINE"] = "1"  # 首次运行请注释掉此行以允许下载模型
os.environ["TRANSFORMERS_NO_ADVISORY_WARNINGS"] = "true"
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="transformers")
warnings.filterwarnings("ignore", category=UserWarning, module="sentence_transformers")
try:
    import transformers
    transformers.logging.set_verbosity_error()
except ImportError:
    pass
# ===============================================

class LLMConfig:
    """大模型配置 (兼容 OpenAI 格式)"""
    def __init__(self, prefix="LLM"):
        self.api_key = os.getenv(f"{prefix}_API_KEY") or os.getenv("OPENAI_API_KEY", "sk-no-key-required")
        self.base_url = os.getenv(f"{prefix}_BASE_URL") or os.getenv("OPENAI_BASE_URL", "http://127.0.0.1:11434/v1")
        self.model = os.getenv(f"{prefix}_MODEL") or os.getenv(f"{prefix}_MODEL_NAME") or os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")
        self.timeout = float(os.getenv(f"{prefix}_TIMEOUT", os.getenv("LLM_TIMEOUT", "60.0")))
        self.max_retries = int(os.getenv(f"{prefix}_MAX_RETRIES", os.getenv("LLM_MAX_RETRIES", "2")))
        self.temperature = float(os.getenv(f"{prefix}_TEMPERATURE", "0.7"))

class EmbedConfig:
    """嵌入模型配置 (支持 Local, Ollama, OpenAI)"""
    type: str = os.getenv("EMBED_TYPE", "local")
    model: str = os.getenv("EMBED_MODEL") or "paraphrase-multilingual-MiniLM-L12-v2"
    base_url: str = os.getenv("EMBED_BASE_URL") or "http://127.0.0.1:11434/v1"
    api_key: str = os.getenv("EMBED_API_KEY") or os.getenv("OPENAI_API_KEY", "sk-no-key-required")

class Neo4jConfig:
    """Neo4j 数据库配置"""
    uri: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    username: str = os.getenv("NEO4J_USER") or os.getenv("NEO4J_USERNAME", "neo4j")
    password: str = os.getenv("NEO4J_PASSWORD")
    database: str = os.getenv("NEO4J_DATABASE", "neo4j")

class MemoryConfig:
    """记忆系统参数"""
    window_size: int = int(os.getenv("MEMORY_WINDOW_SIZE", "6"))
    event_confidence_threshold: float = float(os.getenv("EVENT_CONFIDENCE_THRESHOLD", "0.7"))
    time_decay_half_life_days: int = int(os.getenv("TIME_DECAY_HALF_LIFE_DAYS", "45"))
    max_input_length: int = int(os.getenv("MAX_INPUT_CHARS", "2000"))
    retrieval_top_k: int = int(os.getenv("RETRIEVAL_TOP_K", "8"))
    retrieval_window_cutoff: int = int(os.getenv("RETRIEVAL_WINDOW_CUTOFF", "6"))
    enable_bm25: bool = os.getenv("ENABLE_BM25", "true").lower() == "true"
    enable_persona_injection: bool = os.getenv("ENABLE_PERSONA_INJECTION", "true").lower() == "true"
    
    # 召回配额 (Budget Control)
    budget_graph: int = int(os.getenv("RETRIEVAL_BUDGET_GRAPH", "3"))
    budget_vector: int = int(os.getenv("RETRIEVAL_BUDGET_VECTOR", "3"))
    budget_bm25: int = int(os.getenv("RETRIEVAL_BUDGET_BM25", "2"))

    # --- Structural Memory Events (v1) ---
    default_currency: str = os.getenv("DEFAULT_CURRENCY", "CNY")
    enable_normalization: bool = os.getenv("ENABLE_NORMALIZATION", "true").lower() == "true"
    decimal_precision: int = int(os.getenv("DECIMAL_PRECISION", "4"))

llm_config = LLMConfig(prefix="LLM")
memory_llm_config = LLMConfig(prefix="MEMORY")
embed_config = EmbedConfig()
neo4j_config = Neo4jConfig()
memory_config = MemoryConfig()

class ServerConfig:
    """后端服务器配置"""
    def __init__(self):
        self.host = os.getenv("SERVER_HOST", "0.0.0.0")
        self.port = int(os.getenv("SERVER_PORT", "8000"))
        self.reload = os.getenv("SERVER_RELOAD", "true").lower() == "true"
        self.ws_auth_required = os.getenv("WS_AUTH_REQUIRED", "true").lower() == "true"
        self.ws_auth_token = os.getenv("WS_AUTH_TOKEN", "")
        self.ws_auth_query_param = os.getenv("WS_AUTH_QUERY_PARAM", "ws_token")
        self.maintenance_token = os.getenv("MAINTENANCE_TOKEN", self.ws_auth_token)

server_config = ServerConfig()

class PostgresConfig:
    """共享 PostgreSQL 配置 — 处理跨系统结构化业务数据同步"""
    host: str = os.getenv("POSTGRES_HOST", "")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    db: str = os.getenv("POSTGRES_DB", "ebbingflow_db")
    user: str = os.getenv("POSTGRES_USER", "ebbingflow_admin")
    password: str = os.getenv("POSTGRES_PASSWORD", "")
    tenant_id: str = os.getenv("TENANT_ID", "")  # 查询时使用的租户 ID

    @classmethod
    def connection_string(cls) -> str:
        return f"postgresql://{cls.user}:{cls.password}@{cls.host}:{cls.port}/{cls.db}"

    @classmethod
    def is_configured(cls) -> bool:
        return bool(cls.host and cls.password)

postgres_config = PostgresConfig()

class SqliteConfig:
    db_path: str = os.getenv("SQLITE_DB_PATH", ".data/ef_history.db")

sqlite_config = SqliteConfig()

class IdentityConfig:
    def __init__(self):
        self.user_id = os.getenv("MASTER_USER_ID", "user_001")
        self.assistant_id = os.getenv("MASTER_ASSISTANT_ID", "assistant_001")
        # 默认文案预设
        self.default_user_name = "主人"
        self.default_asst_name = "Andrew"
        self.default_asst_persona = "精致且保持警觉的私人管家"
        # 兼容字段（供不同版本的 prompt 组装器使用）
        self.user_aliases = os.getenv("USER_ALIASES", "无")
        self.assistant_aliases = os.getenv("ASSISTANT_ALIASES", "无")
        self.assistant_role = os.getenv("ASSISTANT_ROLE", "全能管家")
        self.assistant_profile = os.getenv("ASSISTANT_PROFILE", self.default_asst_persona)
        # --- CRM Sync (Phase-2.5) ---
        self.enable_crm_sync = os.getenv("ENABLE_CRM_SYNC", "false").lower() == "true"
        self.crm_source_weight = float(os.getenv("CRM_SOURCE_WEIGHT", "0.65"))

        # --- SQL History & Evidence Chain (Phase-M1) ---
        self.chat_history_backend = os.getenv("CHAT_HISTORY_BACKEND", "sql").lower()
        self.evidence_injection_enabled = os.getenv("EVIDENCE_INJECTION_ENABLED", "true").lower() == "true"
        self.evidence_window_prev = int(os.getenv("EVIDENCE_WINDOW_PREV", "1"))
        self.evidence_window_hit = int(os.getenv("EVIDENCE_WINDOW_HIT", "1"))
        self.evidence_window_recent = int(os.getenv("EVIDENCE_WINDOW_RECENT", "2"))

identity_config = IdentityConfig()
