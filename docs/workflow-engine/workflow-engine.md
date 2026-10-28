# 工作流执行引擎

## 工作流执行器——WorkflowExecutor

负责：

接收工作流定义 + 输入参数，创建 WorkflowRun 实例

按拓扑顺序逐层执行节点（同层并行）

每执行完一个节点，保存 Checkpoint（断点续跑）

处理超时、重试、降级、条件分支

将最终结果写入 WorkflowRun.output

### 单节点执行——executeNodeWithRetries

```java
private void executeNodeWithRetries(WorkflowDefinition def, WorkflowRun run,
                                    String nodeId, WorkflowNode node,
                                    Deadline deadline, NodeTrace trace) {
    // 排队等信号量的节点可能在上游已经超时 / 失败之后才轮到。
    // 此时不要再开始执行，更不要执行完再提交结果 —— 直接把这次执行当作没发生过。
    if (isTerminal(run.getStatus())) {
        log.warn("[Executor] node not started: run already terminal | runId={} | node={} | status={}",
                run.getRunId(), nodeId, run.getStatus());
        return;
    }

    WorkflowRun.NodeExecutionResult result = WorkflowRun.NodeExecutionResult.builder()
            .nodeId(nodeId)
            .nodeName(node.getName())
            .status(WorkflowRun.NodeExecutionResult.NodeStatus.RUNNING)
            .startedAt(Instant.now())
            .build();

    run.getActiveNodeIds().add(nodeId);
    run.addTimelineEvent("NODE_START", nodeId, "开始执行: " + node.getName());
    log.info("[Executor] node start | runId={} | node={} | type={}",
            run.getRunId(), nodeId, node.getNodeType());

    int attempt = 0;
    Exception lastError = null;

    // 重试循环
    while (attempt <= node.getMaxRetries()) {
        // 第一次尝试失败后，兄弟节点可能已经把工作流置成终态了（比如某个节点抛了致命异常，工作流状态变为 FAILED）
        if (isTerminal(run.getStatus())) {
            log.warn("[Executor] node retry aborted: run already terminal | runId={} | node={} | status={}",
                    run.getRunId(), nodeId, run.getStatus());
            return;
        }
        try {
            if (attempt > 0) {
                log.info("[Executor] node retry {}/{} | runId={} | node={}",
                        attempt, node.getMaxRetries(), run.getRunId(), nodeId);
                // span 事件而不是子 span：重试是同一节点的时间点，不是独立工作。
                trace.retry(attempt, node.getMaxRetries());
                Thread.sleep(node.getRetryDelaySeconds() * 1000L);
            }

            // 获取节点执行器
            NodeExecutor executor = nodeExecutorFactory.get(node.getNodeType());
            Map<String, Object> nodeInput = buildNodeInput(def, run, node);

            // 提交到节点执行线程
            // 必须用 ExecutorService.submit() 而不是 CompletableFuture.supplyAsync()
            // 因为前者返回的是 Future(FutureTask) 后者返回的是 CompletableFuture
            // 而 CompletableFuture.cancel() 不能真正的终止线程（只改状态）
            Future<Map<String, Object>> future = nodeCallExecutor.submit(
                    FlowAgentContext.wrap(() -> executor.execute(node, nodeInput, run)));

            // 节点超时不得超过工作流剩余预算：否则一个 timeout=600s 的节点能让 globalTimeoutSeconds=60 的工作流硬拖 10 分钟。
            long nodeTimeout = Math.max(1,
                    Math.min(node.getTimeoutSeconds(), deadline.remainingSeconds()));

            Map<String, Object> nodeOutput;
            try {
                nodeOutput = future.get(nodeTimeout, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // 等待节点结果时被中断（关闭线程池、外部取消）：先取消在途任务，再把中断交给外层专用 catch，绝不进入重试。
                future.cancel(true);
                throw e;
            } catch (TimeoutException e) {
                // 硬取消：interrupt 正在执行的线程，让它别再占着 nodeCallExecutor 空转。
                // 能否真正停下仍取决于节点实现是否响应中断
                // sleep/wait/可中断 IO 会立即抛 InterruptedException
                // 但纯 CPU 循环或不可中断的 socket 读不会。
                future.cancel(true);
                // 超时 → 转为 FlowAgentException 进入重试/降级流程
                throw new FlowAgentException("NODE_TIMEOUT",
                        "节点 " + node.getName() + " 执行超时 (" + nodeTimeout + "s)");
            } catch (ExecutionException e) {
                // 执行异常 → 解包原始异常（节点执行出错了）
                Throwable cause = e.getCause();
                if (cause instanceof FlowAgentException) {
                    throw (FlowAgentException) cause;
                }
                if (cause instanceof Exception) {
                    throw (Exception) cause;
                }
                throw new FlowAgentException("NODE_ERROR",
                        "节点执行异常: " + cause.getMessage());
            }

            // 成功
            result.setStatus(WorkflowRun.NodeExecutionResult.NodeStatus.COMPLETED);
            result.setOutput(nodeOutput);
            result.setInput(nodeInput);
            result.setCompletedAt(Instant.now());
            result.setDurationMs(
                    Duration.between(result.getStartedAt(), result.getCompletedAt()).toMillis());
            result.setRetryCount(attempt);

            commitNodeSuccess(def, run, nodeId, result, nodeOutput);

            log.info("[Executor] node complete | runId={} | node={} | duration={}ms",
                    run.getRunId(), nodeId, result.getDurationMs());
            trace.success(attempt);
            return;

        } catch (InterruptedException e) {
            // Thread.sleep() 被中断时，会抛出 InterruptedException，但同时会把中断标志清掉
            // 手动恢复——因为外层可能还需要知道 "这个线程被中断过"。如果不恢复，中断信号就丢了 —— 上层以为一切正常，继续往下跑
            Thread.currentThread().interrupt();
            trace.failed(e, attempt);
            throw new FlowAgentException("NODE_INTERRUPTED",
                    "节点 " + node.getName() + " 执行被中断", 500, e);
        } catch (Exception e) {
            // 挂起异常 —— 绝不重试
            // 挂起是人工审批节点抛的 —— 它已经建好了审批任务，在等审批结果。如果当失败重试，会再建一个审批任务，一次挂起变成 N 个待审批
            if (e instanceof WorkflowSuspendedException suspended) {
                trace.suspended(suspended);
                throw suspended;
            }
            lastError = e;
            attempt++;
            log.warn("[Executor] node error (attempt {}/{}) | runId={} | node={} | error={}",
                    attempt, node.getMaxRetries() + 1, run.getRunId(), nodeId, e.getMessage());
        }
    }

    // 所有重试都失败 → 降级策略
    trace.failed(lastError, Math.max(0, node.getMaxRetries()));
    settleNodeFailure(def, run, nodeId, node, result, lastError);
}
```

`Thread.sleep()` 被中断时，会抛出 `InterruptedException`，但同时会把中断标志清掉。

`Thread.currentThread().interrupt();` 为什么要加这一行：因为外层可能还需要知道 "这个线程被中断过"。如果不恢复，中断信号就丢了 —— 上层以为一切正常，继续往下跑。

### 层间执行——executeLayer

```java
private void executeLayer(WorkflowDefinition def, WorkflowRun run,
                          List<String> nodeIds, Deadline deadline) {
    // 先查全局超时
    if (deadline.expired()) {
        markGlobalTimeout(def, run);
        return;
    }

    if (nodeIds.size() == 1) {
        // 单个节点直接执行（避免线程池开销）
        executeSingleNode(def, run, nodeIds.get(0), deadline);
        return;
    }

    // 并发闸门：同一时刻最多 maxConcurrency 个节点在执行。
    int maxConcurrency = Math.max(1, def.getConfig().getMaxConcurrency());
    Semaphore permits = new Semaphore(maxConcurrency);
    log.debug("[Executor] layer concurrency gate | runId={} | nodes={} | maxConcurrency={}",
            run.getRunId(), nodeIds.size(), maxConcurrency);

    // 多节点并行执行
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (String nodeId : nodeIds) {
        futures.add(CompletableFuture.runAsync(
                // FlowAgentContext.wrap() 把当前线程的上下文传到工作线程，否则子线程拿不到
                FlowAgentContext.wrap(() -> {
                    try {
                        // 层内节点数可能大于 maxConcurrency，超出的需要排队
                        permits.acquire();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(ie);
                    }
                    try {
                        executeSingleNode(def, run, nodeId, deadline);
                    } finally {
                        permits.release();
                    }
                }),
                layerExecutor));
    }

    // 等待所有节点完成，但不超过工作流剩余预算
    CompletableFuture<Void> all =
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    try {
        if (deadline.unlimited()) {
            all.join();
        } else {
            all.get(Math.max(1, deadline.remainingSeconds()), TimeUnit.SECONDS);
        }
    } catch (TimeoutException te) {
        // 全局超时：放弃等待本层，把工作流置为失败。
        // 这里 cancel 的是编排层的 CompletableFuture，只能阻止「尚未开始」的节点，无法中断已在跑的节点（CompletableFuture 不支持 interrupt）
        // 真正的止血靠 executeSingleNode 里把节点超时钳制到剩余预算，节点会自行超时退出。
        futures.forEach(f -> f.cancel(true));
        markGlobalTimeout(def, run);
    } catch (InterruptedException ie) {
        futures.forEach(f -> f.cancel(true));
        Thread.currentThread().interrupt();
        throw new CompletionException(ie);
    } catch (ExecutionException ee) {
        // 单节点的异常已在 executeSingleNode 内部消化（重试/降级），
        // 能冒到这里的是编排层自身的异常，不应被吞掉
        throw new CompletionException(ee.getCause() == null ? ee : ee.getCause());
    }
}
```

涉及到的并发模型

1.信号量——Semaphore

2.异步编排——并行提交+统一等待

把 N 个节点提交成 N 个 Future，然后 `allOf` 等它们全部完成。不同于 `ExecutorService.invokeAll()` 的阻塞式 

`CompletableFuture.runAsync(..., layerExecutor)`和`CompletableFuture.allOf(futures).get(timeout)`

3.ThreadLocal 上下文传播

`FlowAgentContext.wrap(() -> { ... })`。主线程里存了 traceId、userId、tenantId（在 ThreadLocal 里），扔到子线程后默认拿不到。`wrap()` 就是在提交前把当前线程的上下文快照复制下来，在子线程执行前设进去，执行完清掉。

4.超时取消

`futures.forEach(f -> f.cancel(true))`

### 核心状态机——executeInternal

抢租约 → 拓扑分层 → 逐层并行跑节点 → 收结果 → 存 checkpoint → 推进下一层。是整个引擎的主循环

```java

```

### 断点恢复——prepareResume

读 Redis checkpoint → 校验双指纹 → 重建内存状态



### 节点调度判断——shouldExecuteNode

上游失败了要不要跳过、条件分支选没选它。决定了 "哪些节点真的会被执行"
