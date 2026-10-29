# 链路追踪与成本监控

## 追踪器——AgentTracer

工作流 / 节点两级 span 树的开关

它回答一个问题：一次工作流执行，从开始到结束，经历了哪些节点、调了几次 LLM、用了多少 token

整体结构：两级 span 树

```plaintext
shturl.cc/rV                    ← startWorkflow，根 span，覆盖整次执行
  ├─ workflow.node (AGENT)      ← startNode / endNode
  │    └─ agent.react           ← Agent 节点内部开的
  │         ├─ llm.call         ← LlmCallTracingObserver 自动开
  │         └─ tool.call        ← recordToolCall 补记
  ├─ workflow.node (JUDGE)
  └─ workflow.node (TOOL)
```

```java

```
