# EbbingFlow 技术白皮书
> **“记忆是 AI 的灵魂。”**  
> **Long-term Cognitive Infrastructure for LLM Agents**

---

## 一、 核心定位：从“鱼类记忆”到“长效认知”

当前大语言模型（LLM）在实际应用中面临的三个致命伤，正是 EbbingFlow 致力于解决的核心痛点：

1.  **上下文墙 (The Context Wall)**：长文本窗口再大，也无法低成本地处理跨度数月的碎片化信息。
2.  **语义漂移 (Semantic Drift)**：原生 RAG 检索仅凭相似度，容易导致“驴唇不对马嘴”的知识碎片拼接。
3.  **人格崩溃 (Persona Instability)**：由于缺乏长期的一致性约束，AI 难以形成并维持一个稳定的人格内核。

**EbbingFlow** 并不是一个简单的 RAG 仓库，它是一套**图谱原生（Graph-Native）**且具备**时间感知（Temporal-Aware）**的认知演化框架。

---

## 二、 架构革新：四位一体的记忆中枢 (Memory Trinity & Reality)

EbbingFlow 采用了全新的记忆分层存储策略，解决了“证据可追溯性”与“逻辑一致性”的结构冲突。

### 1. 结构化现实层 (Reality Layer - SQL Structured)
*   **定位**：系统的“理性/数据”中枢。
*   **功能**：负责资产（Finance）、健康（Health）、待办（Plan）等 12 大关键类别的强结构化存储。
*   **核心逻辑**：内置 Normalization Engine 规则清洗，提供 100% 精确的聚合统计能力，解决 AI 处理数字时的“幻觉”问题。

### 2. 结构化事实层 (Fact Layer - Neo4j)
*   **定位**：系统的“感性/逻辑”中枢。
*   **功能**：捕捉实体间复杂的、带权重的强关联。它回答“A 与 B 的具体关系是什么”，并负责维护系统的 **统一分层身份引擎 (Unified Multi-layer Identity Engine)**。

### 3. 证据与历史层 (Evidence Layer - SQL)
*   **定位**：系统 100% 可审计的“黑匣子”。
*   **功能**：所有的原始对话记录通过 **SQL (Postgres/SQLite)** 持久化。
*   **关键特性**：通过 `source_msg_id` 强制关联，确保所有结构化事实和图谱节点都能精准回溯到原始对话现场。

### 4. 泛化知识层 (Classic RAG Layer - Vector)
*   **定位**：系统的“广角检索”与“知识背景”区。
*   **功能**：用于处理海量非结构化文档（如 PDF、技术手册）的快速语义索引。

---

## 三、 核心技术方案：类人记忆的三个维度

### 1. 双时态版本控制 (Bitemporal Awareness)
EbbingFlow 引入了专业的双时态逻辑：
*   **有效时间 (Valid Time)**：某件事在真实世界发生的时间。
*   **记录时间 (Record Time)**：系统学习到这条记忆的时间。
从而解决了“AI 如何处理纠错信息”的问题（例如：用户今天纠正了昨天说错的话）。

### 2. 递归式身份演化 (Recursive Identity Evolution)
系统具备“性格自我修正”能力。核心引擎会定期执行 **Persona Re-inference**：
*   **快变量 (EFSTB Tags)**：捕捉实时的情绪、紧急程度和颗粒度偏好。
*   **慢变量 (Big Five/MBTI)**：基于历史交互沉淀，动态调整长期的性格模型。

### 3. MDRS 记忆综合评分算法
不再单纯依赖语义相似度（Cosine Similarity），而是引入了：
*   **语义相关度 (Relevancy)**
*   **衰减因子 (Recency/Decay)**：模拟艾宾浩斯遗忘曲线。
*   **信任评分 (Trust Score)**：基于证据链的强度决定记忆的牢固程度。

---

## 四、 商业版蓝图：从认知到价值 (Spending Aggregation)

EbbingFlow 的商业化闭环首先切入 **“支出聚合与财务审计”**。利用其强大的“证据追溯”能力，为高净值用户提供一个：
*   **零输入**：通过聊天自动提取开销。
*   **100% 审计**：每笔钱怎么花的，都能通过 `source_msg_id` 一键回跳至当时的聊天证据。
*   **长效分析**：基于跨年度记忆，分析用户的消费习惯演化。

---

## 五、 后语：逻辑编排的艺术

AI 的未来不只是算力的堆砌，更是逻辑的艺术。EbbingFlow 证明了：通过对现有存储技术的深度编排与认知对齐，我们完全可以赋予 LLM 一颗“过目不忘”且“懂人情世故”的心。

> **EbbingFlow：让 AI 找回属于它的灵魂。**

---
*Last Updated: 2026-04-10 (Post-Schema Unification Update)*
