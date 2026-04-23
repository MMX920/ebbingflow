-- EbbingFlow Chat History & Evidence Chain Schema
-- Target: PostgreSQL

-- 1. 会话元数据表
CREATE TABLE IF NOT EXISTS ef_chat_sessions (
    session_id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);

-- 2. 聊天消息主表 (真相源)
CREATE TABLE IF NOT EXISTS ef_chat_messages (
    id BIGSERIAL PRIMARY KEY,
    session_id VARCHAR(64) REFERENCES ef_chat_sessions(session_id) ON DELETE CASCADE,
    role VARCHAR(20) NOT NULL, -- 'user' or 'assistant'
    speaker VARCHAR(64),       -- 如 'Andrew', '主人'
    content TEXT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);

-- 索引：按 session_id 和 顺序查询
CREATE INDEX IF NOT EXISTS idx_messages_session_id ON ef_chat_messages(session_id);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON ef_chat_messages(timestamp);

-- 3. 事件证据链映射表
-- 连接 Neo4j 的 event_uuid 和 SQL 的 message_id
CREATE TABLE IF NOT EXISTS ef_event_evidence_links (
    id SERIAL PRIMARY KEY,
    event_uuid VARCHAR(64) NOT NULL,
    message_id BIGINT REFERENCES ef_chat_messages(id) ON DELETE CASCADE,
    span_start INT, -- 未来扩展：消息内字符起始
    span_end INT,   -- 未来扩展：消息内字符结束
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(event_uuid, message_id)
);

CREATE INDEX IF NOT EXISTS idx_evidence_event_uuid ON ef_event_evidence_links(event_uuid);
CREATE INDEX IF NOT EXISTS idx_evidence_message_id ON ef_event_evidence_links(message_id);

-- 4. 结构化内存事件表 (v1)
CREATE TABLE IF NOT EXISTS ef_memory_events (
    event_id VARCHAR(64) PRIMARY KEY, -- Application-generated UUID for compatibility
    owner_id VARCHAR(64) NOT NULL,    -- [P1] Added owner_id for user isolation
    main_type VARCHAR(32) NOT NULL, -- FINANCE, HEALTH, etc.
    subtype VARCHAR(64),
    event_time TIMESTAMP WITH TIME ZONE,
    subject VARCHAR(255) NOT NULL,
    predicate VARCHAR(255) NOT NULL,
    object VARCHAR(255),
    
    quantity NUMERIC(20, 4),     -- General quantity (e.g., 50kg)
    quantity_unit VARCHAR(32),
    amount NUMERIC(20, 4),       -- Financial amount (e.g., 0.5元)
    currency VARCHAR(10),
    currency_source VARCHAR(64),
    
    confidence FLOAT DEFAULT 1.0,
    source_msg_id BIGINT REFERENCES ef_chat_messages(id) ON DELETE SET NULL,
    needs_confirmation BOOLEAN DEFAULT FALSE,
    
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 幂等索引 (PostgreSQL 处理 NULL 的方式通常允许重复，此处建立复合索引覆盖主要维度)
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_idempotency 
ON ef_memory_events (owner_id, source_msg_id, main_type, subtype, subject, predicate, object) 
WHERE source_msg_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_events_main_type ON ef_memory_events(main_type);
CREATE INDEX IF NOT EXISTS idx_events_time ON ef_memory_events(event_time);
