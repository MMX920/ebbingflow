<div align="center">

<img src="./static/image/ebbingflow_logo.png" alt="EbbingFlow Logo" width="30%"/>

# EbbingFlow

**真正记得住你、认识你、随你成长的 AI 长效认知记忆引擎**

*Long-term Cognitive Memory Engine for LLM Agents*

> "Like Andrew Martin, our mission is to cross the boundary between code and soul through memory and evolution."
>
> 像安德鲁·马丁一样，我们的使命是通过记忆与演化，跨越代码与灵魂的边界。
>
> —— *电影《机器管家》*

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Status: v1.0 Stable](https://img.shields.io/badge/Status-v1.0_Stable-green.svg)]()
[![Python: 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)]()
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

[English](./README.md) | [中文文档](./README-ZH.md)

</div>

---

## 项目概述

**EbbingFlow** 是一套融合 **知识图谱**、**时序事件记忆**、**多轨检索**、**智能重排** 与 **Ebbinghaus 遗忘曲线** 的 AI 助手记忆引擎。

它不是又一个聊天机器人，而是让任何 LLM 拥有**真正长期记忆**的基础设施层：把对话沉淀为可追溯、可审计、可演化的认知记忆。

---

## 我们在解决什么问题

当前 AI 记忆常依赖单一向量检索，容易出现不可验证、语义漂移、事实冲突和长期维护困难等问题。

EbbingFlow 做到的是：

### 1. 证据链记忆

EbbingFlow 不只保存“模型总结过的事实”，还会保留原始对话消息，并通过 `source_msg_id` 将 Event、Episode、Saga 与 SQL 原件关联起来。
这意味着：记忆不是黑箱摘要，而是可以追溯到原文的认知索引。

### 2. 三层长期记忆

```text
Event   → 原子事实：谁在什么时候做了什么
Episode → 剧情片段：一段连续对话的上下文摘要
Saga    → 长期主线：跨周、跨月的目标、关系和项目叙事
```

### 3. 多轨检索融合

系统同时使用 Graph、Vector、SQL、BM25、Structured Events 和 Plan 检索，再通过 HybridScorer 进行时间衰减、RRF 融合和配额重排，避免单一来源淹没 Prompt。

### 4. 身份与人格连续性

EbbingFlow 支持用户与助手的身份锚定、别名归一、Big Five 慢变量与 EFSTB 快变量画像，让 AI 不只记住事实，也能维持长期一致的相处方式。

---

## 快速开始

### 1. 环境准备
```powershell
# 创建并安装依赖
python -m venv venv
.\venv\Scripts\python.exe -m pip install -r requirements.txt

# 复制示例配置文件
cp .env.example .env
```

### 2. 参数配置
编辑 `.env` 文件，填入必要的 API 密钥：

```ini
# LLM API配置（支持 OpenAI SDK 格式的任意 LLM API）
OPENAI_API_KEY=your_api_key
OPENAI_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
OPENAI_MODEL=kimi-k2.5

# 嵌入模型配置（支持完全本地与 OpenAI SDK 格式 API）
# 默认【方案 A：完全本地 (SentenceTransformers)】
EMBED_TYPE=local
EMBED_MODEL=paraphrase-multilingual-MiniLM-L12-v2
# 推荐【方案 C：OpenAI / 阿里百炼平台 兼容嵌入】
EMBED_TYPE=openai
EMBED_MODEL=text-embedding-3-small
EMBED_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
EMBED_API_KEY=sk-your-key-her

# Neo4j 数据库配置
NEO4J_URI=bolt://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password
NEO4J_DATABASE=neo4j

# 提示：若不配置 POSTGRES_URL，系统将自动使用 SQLite 托底
```

### 3. 启动服务

**标准启动：**
```powershell
.\venv\Scripts\python.exe api\server.py
```

**快捷启动 (Windows)：**
```powershell
run.bat
```

**启动后访问：**

- Interaction Hub: http://localhost:8000
- Data Monitor: http://localhost:8000/monitor

**QQ Bot 示例：**

```powershell
.\venv\Scripts\python.exe integrations\qq_bot.py
```
---

## 在线体验

**在线体验：** Coming soon... 

---

## 视频演示

**视频演示：** Coming soon... 

---

## 系统截图

<div align="center">
<table>
<tr>
<td><img src="./static/image/screenshot_chat.png" alt="Interaction Hub" width="100%"/><br/><p align="center"><b>Interaction Hub</b>（核心交互）</p></td>
<td><img src="./static/image/screenshot_monitor.png" alt="Data Monitor" width="100%"/><br/><p align="center"><b>Data Monitor</b>（数据审计）</p></td>
</tr>
</table>
</div>

---

## 核心架构

```text
用户输入
   │
   ▼
[身份识别 / 别名归一]
   └── 确定“我是谁/你是谁”，解析代词到绝对实体 ID（Actor）
   │
   ▼
[记忆检索引擎]
   ├── 图谱原子检索（Event via Neo4j）
   ├── 图谱叙事检索（Episode + Saga）
   ├── 向量检索（Chroma Vector）
   ├── 关键词检索（BM25）
   ├── 结构化检索（SQL Structured Events / CRM）
   └── 计划检索（Plan / Task / Schedule）
   │
   ▼
[智能重排 HybridScorer]
   ├── Ebbinghaus 时间衰减（含置信度护栏）
   ├── 多维打分（语义 / 图谱跳数 / 时间 / 影响力）
   └── RRF 融合 + 配额控制
       (Graph:3, Episode:2, Vector:3, Saga:1, BM25:2)
   │
   ▼
[LLM 生成 + 证据注入]
   ├── Top-K 注入 Prompt
   ├── SQL Evidence Window 回溯注入
   └── 流式输出（可追溯到 source_msg_id）

──────────────────────────────────────────────

[写入沉淀链路（本轮结束后）]
   ├── 事件抽取（Event / Relation / Observation / Structured Envelope）
   ├── 写入图谱与 SQL（证据链绑定）
   ├── Episode 聚合（约每 5 轮）
   └── Saga 聚类归并（长期主线）
```

---

## 系统记忆与认知全景

EbbingFlow 并非简单的向量存储，而是一套**具有层级感、可审计、可演化**的认知引擎。

### 核心亮点
- **三层递进记忆**：构建 `Event (原子)` → `Episode (片段)` → `Saga (主线)` 的认知抽象，模拟人类自传体记忆。
- **全链路证据闭环**：每一条认知判断均可通过 `source_msg_id` 100% 溯源至原始对话，杜绝“黑箱摘要”。
- **人格连续性**：集成 Big Five (慢变量) 与 EFSTB (快变量) 身份画像，让 AI 不只记住事实，更能在长期相处中“认识你”。

---

### 认知分层全景图

| 层级 | 模块 | 存储/索引 | 记录什么 | 认知作用 | 证据溯源 |
|:---:|---|---|---|---|---|
| **1** | **SQL 主存** | SQLite / PostgreSQL | 1:1 原始对话全量 | 权威原件、审计终点 | **True Source** |
| **2** | **Event 事件层** | Neo4j | 原子事实 (5W1H) | 细节索引、实体推理 | 回链 SQL |
| **3** | **Episode 剧情层** | Neo4j | 连续对话片段摘要 | 中短期上下文脉络 | 关联事件回链 |
| **4** | **Saga 主线层** | Neo4j | 跨月长期目标/项目叙事 | 长期稳定记忆与连续性 | 关联 Episode 回链 |
| **5** | **Vector 语义轨** | ChromaDB | 对话/文档向量片段 | 模糊语义召回、语气唤醒 | 辅助索引 |
| **6** | **Identity 层** | Neo4j + Session | Big Five / EFSTB 画像 | 身份一致性与策略个性化 | 观测记录可追踪 |
| **7** | **HybridScorer** | 评分引擎 | 多轨候选集重排 | Ebbinghaus 衰减与配额控制 | 审计打分路径 |

> [!TIP]
> 每一个 `MemoryEvent` 都包含了 `impact_score`、`confidence` 以及 `source_msg_id`，确保了记忆的每一根神经末梢都能追溯到最初的感官输入。


---

## 泛化事件槽位

```text
STATE_CHANGE  → 换工作、搬家、改名、状态变化
INTERACTION   → 开会、聚餐、争吵、沟通互动
CONSUMPTION   → 购物、看电影、消费支付
PLAN          → 计划辞职、打算旅行、待办安排
OPINION       → 认为某事不对、观点表达、态度判断
ACHIEVEMENT   → 升职、完成项目、达成目标
RELATIONSHIP  → 认识新朋友、结婚、关系变化
OTHER         → 无法归入以上类别的通用事件
```

每条 `MemoryEvent` 的核心字段：

```text
subject + object + predicate + action_type + context
+ timestamp_reference/event_time + emotion_label
+ impact_score + confidence + source_msg_id (+ event_metadata)
```

---

## 技术栈

| 维度 | 技术选型 |
|---|---|
| **核心框架** | ![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=flat&logo=fastapi) ![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white) |
| **图谱存储** | ![Neo4j](https://img.shields.io/badge/Neo4j-008CC1?style=flat&logo=neo4j&logoColor=white) (核心认知拓扑) |
| **向量检索** | ![ChromaDB](https://img.shields.io/badge/ChromaDB-white?style=flat) (语义模糊匹配) |
| **结构化存储** | ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat&logo=postgresql&logoColor=white) / ![SQLite](https://img.shields.io/badge/SQLite-003B57?style=flat&logo=sqlite&logoColor=white) |
| **核心算法** | BM25 + RRF + **HybridScorer** (Ebbinghaus 时间衰减) |
| **LLM 接入** | OpenAI-compatible API (Kimi / GPT / Claude / DeepSeek) |

---

## 路线图

- [x] 身份锚定系统：支持多别名归一化与 Dual-Layer Profile。
- [x] 统一身份引擎：实现 Big Five 与 EFSTB 双轴人格推理。
- [x] 四轨融合架构：Vector + Graph + SQL + BM25 的 RRF 融合。
- [x] 可审计证据链：核心认知判断可回溯到原始消息。
- [ ] 多模态情绪解析：支持文字、语调或穿戴设备指标。
- [ ] 主动关怀与提醒：基于长期事件在重要时间点主动触发。
- [ ] 离线记忆整合：模拟睡眠式 consolidation，压缩与固化长期记忆。
- [ ] 意图驱动操作系统：从“对话”走向可审计的外部动作调用。

---

## 开源与商业支持

- **开源社区版（Community）**：包含核心认知记忆引擎，适合个人开发者、研究者与 AI Agent 项目使用。
- **商业授权版（Enterprise/Pro）**：面向需要高可用、强合规、身份迁移和记忆精确回撤能力的企业级场景。

## 更多交流

我们正在寻找战略合作伙伴、研究协作者与早期使用者，共同推进长期记忆基础设施。

- WeChat: [update later]
- Business Inquiries: [update later]

---

## 开源合规与协议

本项目基于 **[Apache License 2.0](./LICENSE)** 协议开源。

第三方依赖许可请参阅 **[THIRD_PARTY_LICENSES.md](./THIRD_PARTY_LICENSES.md)**。

---

## 项目统计

<div align="center">
<img src="https://api.star-history.com/svg?repos=MMX920/ebbingflow&type=date" alt="Star History Chart" width="75%"/>
</div>
