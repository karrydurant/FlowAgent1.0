# 工作流执行引擎

## [WorkflowExecutor.java]

工作流执行器

负责：

接收工作流定义 + 输入参数，创建 WorkflowRun 实例

按拓扑顺序逐层执行节点（同层并行）

每执行完一个节点，保存 Checkpoint（断点续跑）

处理超时、重试、降级、条件分支

将最终结果写入 WorkflowRun.output

并发模型：

使用 ExecutorService 线程池执行节点任务。每个节点在独立线程中运行，同层节点可并行执行。等待同层所有节点完成后，才推进到下一层。

```java
package com.flowagent.workflow.executor;

import com.flowagent.common.exception.FlowAgentException;
import com.flowagent.common.metrics.FlowAgentMetrics;
import com.flowagent.common.model.FlowAgentContext;
import com.flowagent.common.tracing.TraceScope;
import com.flowagent.observe.tracing.AgentTracer;
import com.flowagent.workflow.checkpoint.CheckpointManager;
import com.flowagent.workflow.dag.DagValidator;
import com.flowagent.workflow.dag.TopologicalSorter;
import com.flowagent.workflow.lease.WorkflowRunLease;
import com.flowagent.workflow.lease.WorkflowRunLeaseManager;
import com.flowagent.workflow.model.*;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

@Slf4j
@Component
@RequiredArgsConstructor
public class WorkflowExecutor {
    private final NodeExecutorFactory nodeExecutorFactory;
    private final CheckpointManager checkpointManager;
    private final AgentTracer agentTracer;

    // 执行租约：保证同一 runId 在任意时刻只有一个执行者
    private final WorkflowRunLeaseManager leaseManager;

    // "还没跑完"的运行索引——供 WorkflowRunSweeper 找出没人管的运行
    private final WorkflowInflightIndex inflightIndex;

    // 业务指标
    @Autowired(required = false)
    private FlowAgentMetrics metrics;

    // 三个线程池的队列深度/活跃数注册进指标
    @PostConstruct
    void registerPoolGauges() {
        if (metrics == null) {
            return;
        }
        metrics.registerPoolMetrics("workflow-layer", (ThreadPoolExecutor) layerExecutor);
        metrics.registerPoolMetrics("workflow-node", (ThreadPoolExecutor) nodeCallExecutor);
        metrics.registerPoolMetrics("workflow-run", (ThreadPoolExecutor) workflowExecutor);
    }

    // 层内编排线程池——只负责 "提交节点任务+等待结果"，不跑节点业务逻辑
    // 这里的线程会阻塞在 future.get(timeout) 上，因此必须与真正执行节点的 nodeCallExecutor 分离。
    private final ExecutorService layerExecutor = new ThreadPoolExecutor(
        4, 64, 60L, TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        r -> new Thread(r, "flowagent-layer-" + UUID.randomUUID().toString().substring(0, 4)),
        new ThreadPoolExecutor.CallerRunsPolicy()
    );

    // 节点执行线程池——真正运行 NodeExecutor#execute 的地方
    private final ExecutorService nodeCallExecutor = new ThreadPoolExecutor(
        8, 64, 60L, TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        r -> new Thread(r, "flowagent-node-" + UUID.randomUUID().toString().substring(0, 4)),
        new ThreadPoolExecutor.CallerRunsPolicy()
    );

    // 工作流执行线程池——异步提交用
    private final ExecutorService workflowExecutor = new ThreadPoolExecutor(
            2, 8, 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(256),
            r -> new Thread(r, "flowagent-workflow-" + UUID.randomUUID().toString().substring(0, 4)),
            new ThreadPoolExecutor.CallerRunsPolicy()
    );

    // 保留的已结束运行实例上限（LRU 淘汰）
    private static final int FINISHED_RUN_RETENTION = 200;

    // 运行中的工作流（runId → WorkflowRun）
    private final ConcurrentHashMap<String, WorkflowRun> runningWorkflows = new ConcurrentHashMap<>();

    // 已结束的工作流，LRU 有界保留
    private final Map<String, WorkflowRun> finishedWorkflows = Collections.synchronizedMap(
            new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, WorkflowRun> eldest) {
                    return size() > FINISHED_RUN_RETENTION;
                }
            }
    );

    // 提交并同步执行一个工作流（阻塞直到执行完成）
    public WorkflowRun execute(WorkflowDefinition definition, Map<String, Object> input) {
        WorkflowRun run = createRun(definition, input);
        executeInternal(definition, run);
        return run;
    }

    // 提交并异步执行一个工作流（立即返回 runId，后台执行）
    // DAG 校验在提交时同步完成，定义非法会立即抛异常，而不会静默进入后台失败。
    public WorkflowRun submit(WorkflowDefinition definition, Map<String, Object> input) {
        WorkflowRun run = createRun(definition, input);
        workflowExecutor.execute(FlowAgentContext.wrap(() -> executeInternal(definition, run)));
        return run;
    }

    /**
     * 从 Checkpoint 恢复一次中断的工作流
     * 三道校验
     * 快照存在：Redis 中有该 runId 的 checkpoint，否则 404
     * 定义未变：WorkflowDefinition fingerprint() 与快照记录的一致。
     * 状态可恢复：只拒绝 COMPLETED（无事可做）与 CANCELLED（显式终止）这两种
     *
     * 不保证节点副作用的事务性
     * 要求节点执行器自身幂等；对不幂等的外部调用（下单、扣款、发通知），应由节点实现携带幂等键，引擎层无法代劳
     */
    public WorkflowRun resume(WorkflowDefinition definition, String runId) {
        WorkflowRun run = prepareResume(definition, runId);
        executeInternal(definition, run);
        return run;
    }

    // 异步版断点续跑：立即返回，后台继续执行
    public WorkflowRun resumeAsync(WorkflowDefinition definition, String runId) {
        WorkflowRun run = prepareResume(definition, runId);
        workflowExecutor.execute(FlowAgentContext.wrap(() -> executeInternal(definition, run)));
        return run;
    }

    // 审批有结果了——把挂起的运行唤醒，按结论继续或走降级
    // 本方法是幂等的，重复调用是安全的。启动前的第一道门会核对「这个 run 是否正停在 nodeId 上等审批
}
```

### "执行租约：保证同一 runId 在任意时刻只有一个执行者"。为什么要这样做？

目的是长时运行任务在进程、节点或发布窗口失效时仍能继续

1.可用性与故障域隔离：

FlowAgent 一次运行是长时任务：全局超时默认 3600 秒，人工审批可以挂 24 小时，一个多节点工作流通常以分钟计。而执行态是有本地性的——run 提交到哪个副本，就在哪个副本的线程池里跑

单副本时，进程崩溃、容器被驱逐、整机下线，所有在跑的 run 都停在原地。恢复方式是人工发现、重启、再等扫描任务把 fa:workflow:inflight 里的运行接回来。

多副本时：状态已经外置（checkpoint、inflight 索引、租约都在 Redis），任一存活副本都能在租约过期后接管。

### 三种线程池

|  | 负责什么 | 阻塞任务队列类型 | 拒绝策略 |
| ---- | ---- | ---- | ---- |
| layerExecutor | 提交节点任务+等待结果 | SynchronousQueue | CallerRunsPolicy |
| nodeCallExecutor | 真正运行节点任务 | SynchronousQueue | CallerRunsPolicy |  
| workflowExecutor | 提交工作流任务，不等跑完立即返回 | LinkedBlockingQueue | CallerRunsPolicy |

