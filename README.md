<div align="center">

<img src="./static/image/ebbingflow_logo.png" alt="EbbingFlow Logo" width="30%"/>

# EbbingFlow
**“记忆是 AI 的灵魂，也是认知的潮汐。”**  
*Long-term Cognitive Memory Infrastructure for LLM Agents*

> "Like Andrew Martin, our mission is to cross the boundary between code and soul through memory and evolution."
> (像安德鲁·马丁一样，我们的使命是通过记忆与演化，跨越代码与灵魂的边界。)
> —— *Inspired by Bicentennial Man*

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Status: vBlock Stable](https://img.shields.io/badge/Status-v1.0_Stable-green.svg)]()
[![Python: 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)]()
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

[English](./README-EN.md) | [中文文档](./README.md)

</div>

---

## ⚡ 项目概述

**EbbingFlow** 是一套具备“类人认知逻辑”的长效记忆基础设施。

我们认为，记忆是 AI 的灵魂。不同于传统的 RAG（仅语义匹配），EbbingFlow 通过模拟人类大脑的 艾宾浩斯衰减与主线聚合机制，结合统一身份建模与确定性证据链，为 AI Agent 提供稳定、可审计且具备真实“情感连续性”的终身认知层。

---

## 🔭 愿景：从对话助手迈向“认知操作系统” (The Vision)

我们相信，拥有记忆的 AI 是人类能力的“数字延伸”。如果说大模型提供了“智力”，技能库给了 AI 灵活的“双手”，那么 EbbingFlow 赋予的就是那个不可或缺的 “认知内核”。

- **🌐 万物互联的神经中枢 (Omni-Interconnectivity Hub)**: 你的 AI 不再仅仅与你对话。
    - **Agent 间协同**: 对 AI 说“告诉小明我晚点到”，小明的 AI 便会收到来自你的信息，双方在秒级内完成通讯互联。
    - **生产力共生**: 深度互联财务软件与 CRM 等。它凭记忆感知每个客户的业务周期与情感偏好，主动提供精准决策支持。
    - **生活主控室**: 从叫车到外卖，AI 记得的不仅是数据，更是你的生活律动。它在无数模块间丝滑穿梭，让“手动操作软件”成为历史。
    - **决策推演辅助**: 基于长期记忆的深度积累，AI 能对你的选择进行“百战模拟”与推演（如：职业选择、生活习惯调整），提供最懂你的风险预判。
    - **真正的私人管家**: 即便你换了手机、换了模型，只要 EbbingFlow 在，你的数字世界和生活律动就在。

---

## 📸 系统截图

<div align="center">
<table>
<tr>
<td><img src="./static/image/screenshot_chat.png" alt="Interaction Hub" width="100%"/><br/><p align="center"><b>Interaction Hub</b> (核心交互)</p></td>
<td><img src="./static/image/screenshot_monitor.png" alt="Data Monitor" width="100%"/><br/><p align="center"><b>Data Monitor</b> (内存审计)</p></td>
</tr>
</table>
</div>

---

## 🛠️ 核心特性与价值 (Core Values & Features)

## 1. 拒绝幻觉：100% 可审计的“存证级”记忆
不同于其他 AI 经常“一本正经地胡说八道”，EbbingFlow 的每一条认知都强制锚定了原始 source_msg_id。

技术支撑：通过 Two-Stage Grounding 技术，你可以一键回溯至聊天原文，彻底消除 AI 幻觉，为隐私决策提供严丝合缝的证据链支撑。

## 2. 人格一致性：让 AI 真正“懂你”
AI 记得的不仅是事实，更是与你交互时的情感色彩。

技术支撑：内置 Unified Identity 引擎，同步追踪性格的“慢变量”（Big Five）与情绪的“快变量”（EFSTB）。无论你如何更改昵称或更换模型，AI 都能通过底层记忆链条锁定同一个主体，维持身份的连续性。

## 3. 全维度的结构化映射：生活的主控室
依托 Saga (主线) -> Episode (剧情) -> Event (事件) 的三层时序架构，将散乱的对话重构为数字孪生数据库。

技术支撑：深度整合 15 种核心事件 Schema（财务、健康、社交、计划等），实现从“对话”到“洞察”的跃迁。它是唯一能精准回答“我上个月总共花了多少钱？”或“我最近的情绪波动规律”的认知引擎。

## 4. 模拟生物记忆：智能遗忘与突触固化
系统的记忆不是死板的数据库，而是像人类大脑一样会“呼吸”。

技术支撑：内置 艾宾浩斯遗忘算法 配合 置信度锁定 (Confidence Guard)。琐碎杂讯会随时间自然淡化，而核心事实会通过不断交互得到“突触增强”。这解决了 AI 存储冗余问题，让 AI 的反应随时间自然演化，越用越像老朋友。

---

## 🛠️ 技术全景 (Technical Deep-Dive)

<details>
<summary><b>点击展开查看 8 大硬核技术特征</b></summary>

### 1. Hybrid-4 混合动力检索引擎
我们不迷信单一算法。针对复杂业务场景，EbbingFlow 采用工业级 RRF (Reciprocal Rank Fusion) 算法，实现了四条轨道的同时并发：

向量轨道 (ChromaDB)：负责处理模糊、感性的情绪共鸣与长文本片段。
图谱轨道 (Neo4j)：支持最高 3 跳 (3-Hop) 的关系推理，自动关联实体脉络（如“傻蛋的哥哥是谁”）。
结构化轨道 (SQL Analytics)：[独家] 支持财务、健康等数据的精确聚合（SUM/AVG），解决 AI 算不准账的痛点。
关键词轨道 (BM25)：作为硬事实兜底，确保序列号、专有名词的 100% 精准召回。

### 2. 艾宾浩斯遗忘曲线与“信任护栏” (Biological Decay & Confidence Guard)
不同于传统的上下文窗口移除，EbbingFlow 模拟生物神经元的突触增强规律：

多维衰减：基于 7 天半衰期的指数衰减模型，让冗余琐碎信息随时间自然下沉。
置信度锁定 (Confidence-based Locking)：[核心逻辑] 高置信度的核心记忆（如姓名、关键承诺）会自动触发“锁定”机制，无视时间衰减，确保 AI 对重要事实的忠诚度。

### 3. 身份自适应引擎 (Unified Identity v3)
我们认为，记忆应当服务于“人”：

双轴建模：通过 Big Five (大五人格) 锚定长期性格稳定性，通过 EFSTB (状态标签) 捕捉短期情绪波动。
检索偏置：系统根据用户的性格特征，自动动态调整记忆检索的侧重（如：对高焦虑用户优先返回舒缓和确定的方案）。

### 4. 三级长程记忆架构
Saga (主线)：刻画用户人生的核心逻辑，永不退磁。
Episode (剧情)：通过时间衰减引导的独立故事线，具备更高的上下文一致性。
Event (原子事件)：毫秒级捕获的最小认知单元，挂接原始对话 ID。

### 5. 100% 可审计透明度 (Evidence Moat)
系统拒绝幻觉。每一条注入 Prompt 的记忆都携带 source_msg_id。通过 Two-Stage Grounding 技术，只有最终入选的记忆会异步拉取其多轮对话原文（Evidence Window），确保 AI 的每一次决策都有迹可循。


</details>

---

## 🚀 快速开始

```bash
# 1. 安装与配置
pip install -r requirements.txt
cp .env.example .env

# 2. 启动全量服务
run.bat

# 启动认知引擎集成网关 (QQ Bot 示例)
python integrations/qq_bot.py
```

---

## 🗺️ 路线图 (Roadmap)

- **🟢 第一阶段：Cortex-Foundation (已达成)**
    - [x] **身份锚定系统**：支持多别名归一化与 Dual-Layer Profile。
    - [x] **统一身份引擎**: 实现 Big Five 与 EFSTB 双轴人格推理。
    - [x] **四栖融合架构**：Vector + Graph + SQL + BM25 的 RRF 融合。
    - [x] **100% 可审计证据链**: 确保所有认知判断皆有据可查。

- **🟡 第二阶段：Cognitive Hub (近期规划)**
    - [ ] **多模态情感染色**: 支持通过文字、语调或穿戴设备指标解析用户情绪，并写入认知槽位。
    - [ ] **纪念日主动关怀**: 基于图谱中的交互事件，在重要日子主动发起对话，建立情感连续性。
    - [ ] **冲突自愈系统 v2**: 引入因果逻辑分析，由 AI 主动发起对话以消解认知矛盾。
    - [ ] **“管家的怀疑”初版**: 确立核心哲学，初步实现基于身份唯一性的逻辑一致性校验。一旦发现冲突，系统会在 System Prompt 中动态开启 `[AUDIT]` 模块，在 AI 的潜意识中植入背景真相与风险

- **🔴 第三阶段：Beyond the Horizon (远方地平线)**
    - [ ] **离线梦境整合 (Consolidation)**: 模拟生物睡眠机制，在后台完成记忆的拓扑折叠与长效固化。
    - [ ] **谈判仿真器 (Shadow Client)**：基于用户历史逻辑，衍生出一个“影子对手”供用户进行无成本的模拟预演。
    - [ ] **镜像社交引擎**: 构建带来源溯因的上帝视角人际网，处理复杂的人际矛盾与逻辑推演。

- **🔮 第四阶段：Omni-Nexus (万物互联与操作系统)**
    - [ ] **意图驱动操作系统**: 完成从“对话”到“动作”的跃迁。AI 将直接驱动 API 调用，实现发信息、付款、调度日历。
    - [ ] **物理世界映射**: 整合 IoT 设备，让 AI 具备现实世界的“时空背景上下文”。
    - [ ] **数字共生体**: 支持导出高强度加密的“灵魂数据包”，实现个人认知与价值观的数字克隆与传承。

---

## 🏢 开源与商业支持 (Commercial Support)

- **开源社区版 (Community)**: 包含核心认知引擎，适合个人开发者与极客实现认知的“从 0 到 1。
- **商业授权版 (Enterprise/Pro)**: 针对需要高可用、强合规的企业级场景。提供 高级身份迁移系统、高保真事实解析模型 以及符合 GDPR 协议的 记忆精准回撤与删除 功能，确保商业环境下的极致稳定性。。

### 🤝 愿景共建 (Join the Journey)
我们正在积极寻找志同道合的战略合作伙伴与愿景投资者，共同加速“认知操作系统”时代的到来。

如果您对 EbbingFlow 的商业化落地、学术研究合作或战略性投资感兴趣，欢迎通过以下渠道与我们开启对话：

📧 WeChat: [update later]
💬 Business Inquiries: [update later]

---

## ⚖️ 开源合规与协议 (Compliance & License)

本项目基于 **[Apache License 2.0](./LICENSE)** 协议开源。详情参阅 **[THIRD_PARTY_LICENSES.md](./THIRD_PARTY_LICENSES.md)**。

---

## 📈 项目统计

<div align="center">
<img src="https://api.star-history.com/svg?repos=MMX920/ebbingflow&type=date" alt="Star History Chart" width="75%"/>
</div>
