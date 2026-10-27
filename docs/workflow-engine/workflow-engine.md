# 工作流执行引擎

## 工作流执行器——WorkflowExecutor

负责：

接收工作流定义 + 输入参数，创建 WorkflowRun 实例

按拓扑顺序逐层执行节点（同层并行）

每执行完一个节点，保存 Checkpoint（断点续跑）

处理超时、重试、降级、条件分支

将最终结果写入 WorkflowRun.output

### 核心状态机——executeInternal

抢租约 → 拓扑分层 → 逐层并行跑节点 → 收结果 → 存 checkpoint → 推进下一层。是整个引擎的主循环

```java

```

### 断点恢复——prepareResume

读 Redis checkpoint → 校验双指纹 → 重建内存状态

### 节点调度判断——shouldExecuteNode

上游失败了要不要跳过、条件分支选没选它。决定了 "哪些节点真的会被执行"
