# 多 Agent 协商引擎

协商引擎：在进程崩溃、副本切换、Agent 宕机、网络抖动等故障场景下，通过状态落盘、幂等投递、崩溃自动接管与并发隔离，保证协商结果不重复、不丢失、不卡死。

作用主要有：

1.组织多轮表态：把议题发给多个 Agent，收齐各自意见；下一轮可以把别人的意见带上，让它们改口或坚持。

2.按规则计票：支持多数、加权、一票否决、法定人数、弃权。

3.管住时间和失败：整场一个总截止时间，慢成员不会拖死全局；某个成员失败或超时，按规则处理，而不是让整场卡住。

4.保证过程可靠：协商状态落盘，协调者崩了能被接手续跑；重复投递不会让同一个 Agent 跑两遍；最后留下完整记录，能说清谁在第几轮说了什么、谁缺席、结论怎么来的。

## 状态字段——NegotiationState

| 字段 | 为什么重要 |
|---|---|
| `status` | 状态机核心，决定当前在哪个阶段（等回复？决策？收尾？） |
| `round` | 当前第几轮，开了几轮了 |
| `pendingTurns` | 还在等谁回复，每个 turn 对应一个 Agent |
| `currentOpinions` | 已经收到的意见，决策时用 |
| `leaseEpoch` | 多副本安全的栅栏，旧主写入被拒绝靠它 |
| `result` | 最终结果（决策、理由） |
| `voterIds` | 参与投票的都有谁 |
| `deadlineAt` | 整场截止时间，到点必须收尾 |

## 业务流程

### 轮次状态机

#### 开下一轮——beginRound

```java
private NegotiationState beginRound(NegotiationState state, StepPlan plan) {
    // 算本轮的时间边界
    long now = now();
    int nextRound = state.getRound() + 1;
    long limit = roundLimit(state, now);
    long roundDeadline = deadlines.roundDeadline(limit, now, properties.getPerRoundTimeoutMs());
    if (roundDeadline <= now) {
        return finishWithoutRound(state, plan, now);
    }

    // 重置本轮的所有状态字段
    state.setRound(nextRound);
    state.setStatus(Status.RUNNING);
    state.setPhase("ROUND_POST");
    state.setRoundStartedAt(now);
    state.setRoundDeadlineAt(roundDeadline);
    state.setRoundStartSeq(sessionStore.currentSeq(state.getSessionId()));
    state.setCursor(state.getRoundStartSeq());
    state.setCurrentOpinions(new ArrayList<>());
    state.setSilentAgents(new ArrayList<>());
    state.setRetriedAgents(new ArrayList<>());
    state.setSkipReasons(new LinkedHashMap<>());
    state.setPollDelayMs(50L);
    state.setPostDeadlineAt(now + Math.max(1000L, properties.getPostWindowMs()));

    // 给每个投票者生成待投递任务
    List<PendingTurn> turns = new ArrayList<>();
    for (String voter : voterIds(state)) {
        turns.add(newTurn(state, nextRound, 1, voter, "ROUND"));
    }
    state.setPendingTurns(turns);

    // 把动作挂到 StepPlan 上
    plan.postTurns = new ArrayList<>(turns);
    plan.scheduleAt = state.getPostDeadlineAt();

    log.info("[Negotiation] 开始第 {} 轮 | negotiation={} | session={} | voters={} | 截止={}ms后",
            nextRound, state.getNegotiationId(), state.getSessionId(), voterIds(state),
            roundDeadline - now);
    return state;
}
```

#### 收回复+补投递+自适应轮询——pollReplies

这个方法在 `WAITING_REPLIES` 状态下被调用，负责收集各 Agent 的回复、处理投递异常、决定是继续等还是进入决策

```java
private NegotiationState pollReplies(NegotiationState state, StepPlan plan) {

    // 从会话里读新消息
    long now = now();
    List<A2aMessage> incoming;
    try {
        incoming = sessionManager.messages(state.getSessionId(), state.getCursor()); // state.getCursor() 是上次读到的消息序号
    } catch (RuntimeException e) {
        return fail(state, "SESSION_UNAVAILABLE", messageOf(e), plan);
    }

    // 遍历消息，匹配并记录意见
    for (A2aMessage message : incoming) {
        state.setCursor(Math.max(state.getCursor(), message.getSeq()));
        // 每条消息带着 replyTo（它是回复哪条 SOLICIT 的），SOLICIT 是“意见征询”消息类型
        String replyTo = message.getReplyTo();
        if (replyTo == null) {
            continue;
        }
        // 拿这个 replyTo 去 pendingTurns 里找对应的投递项
        PendingTurn turn = state.pendingTurnFor(replyTo);
        if (turn == null) {
            continue;
        }
        if (!turn.getAgentId().equals(message.getFromAgentId())) {
            // replyTo 是发送方自填的，不可信。必须同时校验 fromAgentId：
            // 只有被征询者本人回复的，才算它的表态；否则丢弃，
            // 避免错配回复顶替掉真正待回复的 turn。
            log.warn("[Negotiation] 回复发送方与 turn 不符，已忽略 | negotiation={} | "
                            + "expected={} | actual={} | messageId={}",
                    state.getNegotiationId(), turn.getAgentId(),
                    message.getFromAgentId(), message.getMessageId());
            continue;
        }
        // 这个 Agent 还没记过意见，就解析后加进去（防止收到重复回复）
        if (state.opinionOf(turn.getAgentId()) == null) {
            state.getCurrentOpinions().add(parseOpinion(state, turn, message));
        }
        // 从 pendingTurns 里删除
        removePending(state, replyTo);
    }

    // 遍历还没回复的 turn，检查投递状态
    // 不只是 "等回复"，还负责发现投递出问题的 turn
    List<PendingTurn> remaining = state.getPendingTurns() == null
            ? List.of() : new ArrayList<>(state.getPendingTurns());
    for (PendingTurn turn : remaining) {
        // 先查会话日志：这条 SOLICIT 消息到底落盘没有？
        boolean persisted = sessionStore.findMessage(state.getSessionId(),
                turn.getMessageId()).isPresent();
        // 消息没落盘（投递可能失败了）
        if (!persisted) {
            String guard = turnGuard.stateOf(turn.getMessageId());
            if (guard != null && guard.startsWith("SUCCEEDED")) {
                // 闸说投过、日志里没有：以日志为准，清掉闸再重投
                turnGuard.remove(turn.getMessageId());
                guard = null;
            }
            if (guard == null || guard.startsWith("FAILED")
                    || turnGuard.isStaleRunning(guard)) {
                // TurnGuard 给每条 turn 维护一个 Redis 槽位，值有 3 种
                // 其中之一 RUNNING:<时间戳> 代表已经有人举手去投这条消息了
                // 任务可能死了却没有标记 FAILED
                // 超过 turn-post-timeout-ms 就当作上一任已死，允许重投
                // 重投本身仍受执行闸的 CAS 保护，同一时刻只有一个执行者
                plan.addRepost(turn);
            }
            continue;
        }
        // 消息落盘了（投出去了，但对方没回）
        if (turn.getPostedAt() == 0) {
            turn.setPostedAt(now);
        }
        // 5s 后 rewake 一次
        boolean stale = now - turn.getPostedAt() > Math.max(1000L, properties.getWakeTimeoutMs());
        // rewakeCount == 0 保证只重唤醒一次，不无限重试
        if (turn.getRewakeCount() == 0
                && (turn.getDeliveryError() != null || stale)) {
            turn.setRewakeCount(1); 
            plan.addRewake(turn);
        }
    }

    // 所有 turn 都收到回复了 → 进入一轮结束的决策逻辑
    if (state.getPendingTurns() == null || state.getPendingTurns().isEmpty()) {
        return decide(state, plan);
    }
    // 到了本轮截止时间→ 没回复的人沿用上一轮表态，然后进入决策
    if (now >= state.getRoundDeadlineAt()) {
        carryForwardSilent(state, now);
        return decide(state, plan);
    }

    // 还在等 → 安排下一次轮询
    // 延迟自适应退避：50ms → 100 → 200 → 400 → 800 → 1000ms
    // 为什么自适应退避：如果一直没人回（对端慢、限流、挂了），还每 50ms 查一次就是空转 —— 每次都要读 Redis、读会话日志、解析空列表，纯属浪费
    long delay = nextPollDelay(state.getPollDelayMs());
    state.setPollDelayMs(delay);
    plan.scheduleAt = Math.min(now + delay, state.getRoundDeadlineAt());
    return state;
}
```

这个方法解决了什么问题：

回复发错了（B回了A的turn）—— fromAgent 校验，丢弃

同一条回复收到多次 —— opinionOf == null 才记录

投递闸说成功了但日志里没有 —— 以日志为准，清闸重投

投递任务死在执行器里，槽停在 RUNNING —— isStaleRunning 超时回收重投

消息发出去了但对端没收到通知 —— 5 秒后 re-wake 一次

有人一直不回复 —— 轮截止后沿用上一轮表态，不无限等

一直没人回复，轮询太频繁 —— 自适应退避 50ms→1000ms

#### 一轮结束的收敛判定——decide

在一轮回复收齐后（或截止）后被调用，负责回答：这一轮结束了，接下来该干什么

```java
private NegotiationState decide(NegotiationState state, StepPlan plan) {
    long now = now();
    List<Opinion> opinions = opinionsOf(state);

    // 执行失败（答复回来了但 success=false）的成员就地补问一次。这种属于没回答好。
    // 处理方式：同一轮内补问一次，然后直接返回（指跳过后续代码执行，因为轮次没结束还没到 decide 的时候） —— 本轮还没结束，等补问的回复。
    // 单纯的慢并不重问
    List<String> failed = failedAttempts(state);
    long elapsed = Math.max(0L, now - state.getRoundStartedAt());
    if (!failed.isEmpty() && now < state.getRoundDeadlineAt()
            && NegotiationDeadlinePolicy.fits(now, elapsed, state.getRoundDeadlineAt())) {
        retryFailed(state, plan, failed);
        return state;
    }

    // 计算关键指标
    state.setRoundMillis(elapsed);
    // 本轮意见的指纹（谁投了什么票、提了什么候选），用来判断和上一轮是否一样
    String fingerprint = NegotiationAggregator.fingerprint(opinions);
    boolean quorum = quorumReached(state);
    boolean unanimous = !isPanel(state) && quorum
            && NegotiationAggregator.unanimous(opinions);
    Optional<Opinion> veto = NegotiationAggregator.vetoing(opinions, vetoSet(state));
    // 连续两轮指纹相同（意见已经收敛了，再谈也不会变）
    boolean stable = stable(state, opinions, fingerprint);
    // 然后把本轮快照写进轮次历史。
    appendRoundRecord(state, fingerprint, unanimous, veto.isPresent(), elapsed, now);

    log.info("[Negotiation] 本轮结束 | negotiation={} | round={} | replied={}/{} | "
                    + "quorum={} | unanimous={} | stable={} | vetoed={} | 用时={}ms",
            state.getNegotiationId(), state.getRound(),
            presentVoters(state).size(), voterIds(state).size(),
            quorum, unanimous, stable, veto.isPresent(), elapsed);

    // 持 VETO 角色的 Agent（比如安全审查）投了反对。立即终止，不等其他轮次。
    if (veto.isPresent()) {
        return finishVeto(state, plan, veto.get().agentId());
    }
    // 超时直接收尾
    if (now >= state.getDeadlineAt()) {
        return finishTimeout(state, plan, "DEADLINE_REACHED", "协商在时限内未完成");
    }

    // 被熔断/舱壁跳过的成员本轮根本没有 turn，再开一轮也只会重复问在场的人：
    // 缺席不是「慢」，重问不会让它出现。缺法定人数时直接收尾并报 no_quorum。
    boolean unreachable = !quorum && state.getSkipReasons() != null
            && !state.getSkipReasons().isEmpty();
    boolean stopRounds = unanimous || stable || unreachable
            || state.getRound() >= maxRounds;
    if (!stopRounds && negotiableAnotherRound(state, now)) {
        return beginRound(state, plan);
    }
    // panel 模式
    if (isPanel(state)) {
        return startSynthesis(state, plan);
    }
    // supervisor 模式
    if (isSupervisor(state)) {
        return startSupervisor(state, plan);
    }
    // 普通投票模式
    return finishAggregate(state, plan);
}
```

### 投递执行器

#### 

#### CAS 成功后的总调度——execute()

状态已经写进 Redis 了，现在把 plan 里登记的动作真正执行掉

做什么：

遍历要投递的 turn，过熔断检查，扔到投递线程池里异步发

遍历要重唤醒的 turn，扔到唤醒线程池

取消上一次安排的定时器（避免旧的定时器触发重复推进）

如果不是终态，按 plan 里的时间安排下一次推进的定时器

如果是终态，做资源清理

```java
private void execute(StepPlan plan) {
    // 遍历要投递的每个 turn，对每个做三件事
    for (PendingTurn turn : plan.postTurns) {
        // 过熔断/舱壁检查
        // 熔断检查：这个 Agent 是不是最近一直在挂
        // 舱壁检查：这个 Agent 当前同时有几条投递正在进行中？是不是超过上限了？
        PeerCircuitBreaker.Skip skip = peers.tryAcquire(turn.getAgentId());
        if (skip != null) {
            // 跳过不是弃权：写进 skipReasons，最终出现在 partyStates 里
            recordSkip(plan, turn, skip);
            continue;
        }
        try {
            // 放进投递线程池
            turnExecutor.execute(() -> postTurn(plan, turn));
        } catch (RejectedExecutionException e) {
            // 刚才那个许可因为投递没真的发生，不该被占着，还回去让后面真正的投递能用
            // 这条 turn 还在状态的 pendingTurns 里，下一轮轮询时会重新检查、重新投递
            peers.release(turn.getAgentId());
            peers.onSettled(turn.getAgentId());
            log.warn("[Negotiation] 投递池已满，turn 留给轮截止/恢复 | negotiation={} | messageId={}",
                    plan.negotiationId, turn.getMessageId());
        }
    }

    // 遍历要重唤醒的 turn
    for (PendingTurn turn : plan.rewakeTurns) {
        try {
            // 扔到专门的唤醒线程池，池满了就跳过这次唤醒 —— 下次轮询还会再检查
            deliverExecutor.execute(() -> rewake(plan, turn));
        } catch (RejectedExecutionException e) {
            log.warn("[Negotiation] 唤醒池已满，跳过本次 re-wake | negotiation={} | messageId={}",
                    plan.negotiationId, turn.getMessageId());
        }
    }

    cancelTimer(plan.negotiationId);

    // 终态清理
    // 摘活跃索引、释放租约和许可、注销信号、关闭会话
    if (plan.finish) {
        NegotiationState state = stateStore.find(plan.negotiationId).orElse(null);
        if (state != null) {
            cleanup(state);
        }
        return;
    }

    // 协商还在中间状态，就在指定时间点安排下一次推进
    if (plan.scheduleAt > 0) {
        long delay = Math.max(1L, plan.scheduleAt - now());
        String id = plan.negotiationId;
        try {
            ScheduledFuture<?> future = timer.schedule(() -> signalBus.signal(id),
                    delay, TimeUnit.MILLISECONDS);
            timers.put(id, future);
        } catch (RejectedExecutionException e) {
            log.warn("[Negotiation] 定时器已关闭，无法安排下一次推进 | negotiation={}", id);
        }
    }
}
```

指定时间是怎么算的？


#### 真正发一条消息——postTurn

它是把一条待投递的 turn 真正发给目标 Agent 的过程

做什么：

1.先抢投递闸，保证同一个 turn 不会被两个线程同时投

2.重新读一次最新状态 —— 因为从创建 turn 到现在，轮次可能已经被推进了，这时候这条 turn 已经过期了，直接标失败放弃

3.根据状态和 turn 构造要发的 A2aMessage

```java
private void postTurn(StepPlan plan, PendingTurn turn) {
    // 抢投递闸
    String token = turnGuard.tryBegin(turn.getMessageId());
    // 抢不到说明这条 turn 已经被别的线程在投了（或者已经投成功过了），直接放弃，归还舱壁许可
    if (token == null) {
        peers.release(turn.getAgentId());
        peers.onSettled(turn.getAgentId());
        return;
    }
    // 读最新状态
    // 因为这条 turn 是在 CAS 之前创建的，从创建到现在可能过了一段时间 —— 状态可能已经变了
    NegotiationState state;
    try {
        state = stateStore.find(plan.negotiationId).orElse(null);
    } catch (RuntimeException e) {
        // 读状态撞上 Redis 故障：任务就这么死在执行器里，没人把槽标成 FAILED，
        // 轮询会一直把它当成「还有人正在投」，直到 turn-post-timeout-ms 后才回收。
        try {
            // 标记失败
            turnGuard.fail(turn.getMessageId(), token, messageOf(e));
        } catch (RuntimeException failError) {
            // 连标失败都做不到（Redis 还没回来）：槽会在超时后被轮询回收
            log.debug("[Negotiation] 标记投递失败也失败了 | negotiation={} | messageId={} | error={}",
                    plan.negotiationId, turn.getMessageId(), failError.toString());
        }
        // 但舱壁许可必须还回去，否则这个 peer 的并发额度就永久少一个。
        peers.release(turn.getAgentId());
        peers.onSettled(turn.getAgentId());
        // 发信号让协调者再推进一次
        // 这样马上就会重新检查 pendingTurns，发现这条是 FAILED，立刻重投。不用等下一次定时器触发。
        signalBus.signal(plan.negotiationId);
        return;
    }
    // 检查状态还有效吗
    if (state == null || state.isTerminal() || state.getRound() != turn.getRound()) {
        turnGuard.fail(turn.getMessageId(), token, "round advanced");
        peers.release(turn.getAgentId());
        peers.onSettled(turn.getAgentId());
        return;
    }
    // 构造消息
    A2aMessage message;
    try {
        message = buildMessage(state, turn);
    } catch (RuntimeException e) {
        // 构造失败标失败、归还许可
        turnGuard.fail(turn.getMessageId(), token, messageOf(e));
        peers.release(turn.getAgentId());
        peers.onSettled(turn.getAgentId());
        return;
    }
    // 异步发送，发完不等结果
    try {
        sessionManager.postAsync(plan.sessionId, message)
                .whenComplete((ignored, error) -> onPosted(plan, turn, token, error));
    } catch (RuntimeException e) {
        onPosted(plan, turn, token, e);
    }
}
```

解决的问题：

1.同一条 turn 会不会被重复投

2.turn 是否还有效

3.出错了如何兜底

#### 投递回调——onPosted

```java
private void onPosted(StepPlan plan, PendingTurn turn, String token, Throwable error) {
    try {
        // 先查会话日志：这条 SOLICIT 消息是否落盘了
        boolean persisted = sessionStore.findMessage(plan.sessionId, turn.getMessageId())
                .isPresent();
        // 正常投递成功或是消息落盘但唤醒对端失败都算"投出去了"
        if (error == null || persisted) {
            turnGuard.succeed(turn.getMessageId(), token);
            if (error != null) {
                markDeliveryError(plan.negotiationId, turn.getMessageId(), messageOf(error));
                peers.onFailure(turn.getAgentId());
            } else {
                markPosted(plan.negotiationId, turn.getMessageId());
                peers.onSuccess(turn.getAgentId());
            }
        } else {
            // 消息根本没发出去，标闸失败，下次轮询会重投。
            turnGuard.fail(turn.getMessageId(), token, messageOf(error));
            markDeliveryError(plan.negotiationId, turn.getMessageId(), messageOf(error));
            peers.onFailure(turn.getAgentId());
        }
    } catch (RuntimeException e) {
        // 确认落盘这一步本身撞上 Redis 故障：不能猜「投过了」，也不能把槽留在
        // RUNNING。标 FAILED 让轮询重投；重投前会先查日志，真落过盘就不会再发。
        turnGuard.fail(turn.getMessageId(), token, messageOf(e));
    } finally {
        // 还舱壁许可
        peers.release(turn.getAgentId());
        // 发信号让协调者继续推进
        signalBus.signal(plan.negotiationId);
    }
}
```

## 多副本安全的实现——NegotiationStateStore的transition方法

这是协商引擎状态写入的唯一通道，所有状态修改都走这个方法。一次状态转换：校验 epoch 与允许的旧状态，改副本，整文档 CAS 写回。

```java
public boolean transition(String negotiationId, long expectedEpoch, Collection<Status> allowed,
                          UnaryOperator<NegotiationState> mutator) {
    Set<Status> permitted = allowed == null || allowed.isEmpty()
            ? EnumSet.noneOf(Status.class)
            : EnumSet.copyOf(allowed);
    RBucket<String> bucket = redissonClient.getBucket(stateKey(negotiationId));

    // CAS 重试循环
    for (int attempt = 0; attempt < Math.max(1, properties.getCasRetries()); attempt++) {
        // 读旧状态
        String oldRaw = bucket.get();
        if (oldRaw == null) {
            return false;
        }
        NegotiationState current;
        try {
            current = JSON.parseObject(oldRaw, NegotiationState.class);
        } catch (RuntimeException e) {
            log.warn("[Negotiation] 状态 JSON 解析失败，放弃本次推进 | negotiation={} | error={}",
                    negotiationId, e.getMessage());
            return false;
        }
        if (current == null) {
            return false;
        }

        // 终态不可覆盖
        // 一场协商已经出了结果，但可能同时发生：
        // 1. 协调者收尾：协商谈完了，写 CONCLUDED + 结果
        // 2. 用户取消：用户点了取消按钮，想改成 CANCELLED
        // 3. 接管耗尽：扫描器接管了 3 次都没成功，判 FAILED
        if (current.getStatus() != null && current.getStatus().terminal()) {
            return false;
        }

        // 读取到的当前 epoch 与本次推进者持有的栅栏令牌不一致，表明该推进者已在接管中失去主身份，本次转换作废。
        if (current.getLeaseEpoch() != expectedEpoch) {
            log.info("[Negotiation] 拒绝过期推进者 | negotiation={} | stateEpoch={} | mine={}",
                    negotiationId, current.getLeaseEpoch(), expectedEpoch);
            return false;
        }

        // 调用方声明“我只在状态是 X 时才做这个转换”
        // 比如只允许从 WAITING_REPLIES 进入决策，如果当前已经是 RUNNING，说明别的推进者已经往前走了，这次作废
        if (!permitted.isEmpty() && !permitted.contains(current.getStatus())) {
            return false;
        }

        NegotiationState next;
        try {
            // 只改内存对象，不做任何网络 I/O, 因为 CAS 失败后会重试，mutator 会被执行多次，修改内存几乎没副作用
            next = mutator.apply(current);
        } catch (RuntimeException e) {
            log.warn("[Negotiation] 状态转换失败 | negotiation={} | status={} | error={}",
                    negotiationId, current.getStatus(), e.getMessage());
            return false;
        }
        if (next == null) {
            return false;
        }
        next.setUpdatedAt(System.currentTimeMillis());

        // 在内存里改好 newRaw
        String newRaw = JSON.toJSONString(next);

        // "读旧值" 到 "写新值" 之间有时间窗口，如果直接写，可能覆盖掉别人在这期间的修改
        if (bucket.compareAndSet(oldRaw, newRaw)) {
            // Redisson 的 CAS 底层是 SET key newRaw，会把原来的 TTL 清掉，所以要补一次
            bucket.expire(Duration.ofSeconds(properties.getStateTtlSeconds()));
            return true;
        }
    }
    log.info("[Negotiation] 状态 CAS 连续失败，放弃本次推进 | negotiation={} | attempts={}",
            negotiationId, properties.getCasRetries());
    return false;
}
```

epoch 栅栏是什么：

epoch 就是一个单调递增的版本号，每次接管就 +1。旧主（当前负责推进这一场协商的那个进程）手里的版本号永远落后，所以它的写入会被自动拒绝 —— 不用锁，不用选举，靠版本号比对就保证了只有当前主能写。

为什么 mutator 方法只修改内存：

防止 CAS 多次重试的副作用

## 崩溃接管的实现——NegotiationSweeper的inspect方法

```java
private boolean inspect (String id) {
    // 先判断这场协商还存在吗
    NegotiationState state=stateStore.find(id).orElse(null);
    if (state == null) {
        stateStore.removeInflight(id);   // 状态过期了，但 inflight 索引里还有它
        orphanSeenAt.remove(id);
        return true;
    }

    // 是否已经结束
    // 终态的协商不需要接管，只需要做资源清理
    if (state.isTerminal()) {
        coordinator.cleanup(state);   // CONCLUDED/TIMED_OUT/FAILED/CANCELLED
        return true;
    }

    // 被授权推进这一场协商的实例是哪个，还活着吗？
    Optional<String> owner = lease.ownerValue(id);
    boolean alive = owner.isPresent()
            && epochOf(owner.get()) == state.getLeaseEpoch()   // 心跳里的 epoch 和状态里的一致
            && state.getLeaseEpoch() > 0;

    if (alive) {
        // 之前观察到无主的记录清掉
        orphanSeenAt.remove(id);
        // 刷新索引里的分数
        stateStore.touchInflight(id);

        // 这个主是我自己
        if (owns(owner.get())) {
            // 给心跳续 TTL
            lease.renew(id, state.getLeaseEpoch());   

            // 我是主，但状态 30 秒没动了——我自己卡住了
            if (state.isWaiting() && now - state.getUpdatedAt() > properties.getStaleAdvanceMs()) {
                // 补一次推进
                coordinator.advance(id);    
                return true;
            }
        }

        // 主活着，但已经过了绝对截止时间
        if (now >= state.getDeadlineAt()) {
            // 推进一次让它自己收尾
            coordinator.advance(id);        
            return true;
        }
        return false;
    }
}
```

## 舱壁隔离与熔断——PeerCircuitBreaker

舱壁隔离：把系统资源（线程池、数据库连接、HTTP 连接池、并发额度）按调用方分开隔离，每个调用方有自己的配额，一个慢调用方最多耗尽自己的配额，不会拖垮整个系统。

熔断：当一个下游服务连续失败，就主动 "断开" 对它的调用

### 放行决策

```java
public Skip tryAcquire(String agentId) {
    if (agentId == null) {
        return null;
    }

    Health h=health.computeIfAbsent(agentId, ignored -> new Health());
    long now=System.currentTimeMillis();

    // 在 Health 锁内判断熔断状态，决定放不放探针
    synchronized (h) {
        // 熔断正在打开中，拒绝一切
        if (h.openUntil > now) {
            return Skip.SKIPPED_CIRCUIT_OPEN;
        }

        // 半开状态
        // 熔断窗口刚过，要放一个探针试试上游恢复了没有
        if (h.openUntil != 0) {
            // 已经有一个探针在飞了，其余请求继续拒绝
            if (h.probeInFlight) {
                return Skip.SKIPPED_CIRCUIT_OPEN;
            }
            // 占住探针位：接下来要去 acquire 舱壁许可，acquire 成功才算探针真的飞出去
            h.probeInFlight = true;
        }
    }

    // 拿舱壁信号量
    Semaphore semaphore=permits.computeIfAbsent(agentId, ignored -> new Semaphore(Math.max(1, properties.getPerPeerMaxInflight())));

    // tryAcquire 非阻塞：拿不到就立刻返回，不等待
    if (!semaphore.tryAcquire()) {
        // 舱壁满了
        // 如果刚才在半开状态占了探针位，但现在舱壁没拿到 → 探针根本没飞出去，必须把探针位还回去
        // 否则这个 peer 会永远卡在半开状态
        synchronized (h) {
            h.probeInFlight = false;
        }
        return Skip.SKIPPED_BULKHEAD;
    }
    return null;
}
```

### 资源归还

```java
// 投递结束后归还舱壁许可
// 无论成功失败，舱壁许可都要归还
public void release(String agentId) {
    if (agentId == null) {
        return;
    }
    Semaphore semaphore = permits.get(agentId);
    // 如果 semaphore 不存在（比如刚 reset 过），不做任何事，避免 NPE
    if (semaphore != null) {
        semaphore.release();
    }
}
```

### 结局回调

#### 一次成功的投递/执行：清零失败计数并关闭熔断。

成功指的是：消息已经成功写入共享 Redis，对方也收到了唤醒通知，整个投递链路是通的。

熔断是为了处理 "持续故障"，不是 "偶发抖动"，所以一次成功就关闭熔断

```java
public void onSuccess(String agentId) {
    if (agentId == null) {
        return;
    }
    Health h = health.computeIfAbsent(agentId, ignored -> new Health());
    synchronized (h) {
        h.failures = 0;            
        h.openUntil = 0;           
        h.probeInFlight = false;   
    }
}
```

#### 一次失败的投递执行

投递动作执行了，但没成功把消息送到对方手里，可能是因为：消息根本没写进共享 Redis，消息写进 Redis 了，但跨实例的 HTTP 唤醒通知没发出去

1.半开状态下的探针失败 → 直接重新打开熔断，不再攒阈值

2.闭合状态下的正常失败 → failures++，达到阈值才打开熔断

```java
public void onFailure(String agentId) {
    if (agentId == null) {
        return;
    }
    Health h = health.computeIfAbsent(agentId, ignored -> new Health());
    long now = System.currentTimeMillis();
    synchronized (h) {
        boolean probing = h.probeInFlight;
        h.probeInFlight = false;

        if (probing) {
            // 探针失败：直接重新打开，不攒阈值
            h.failures = 0;
            h.openUntil = now + Math.max(1L, properties.getCircuitOpenMs());
            return;
        }

        // 正常失败：连续计数
        h.failures++;
        if (h.failures >= Math.max(1, properties.getCircuitFailureThreshold())) {
            h.failures = 0;
            h.openUntil = now + Math.max(1L, properties.getCircuitOpenMs());
        }
    }
}
```

#### 一次 "没有真正抵达 peer" 的收尾

消息没有真正发出去，不是接收方的问题，是投递侧问题，典型场景：

投递闸显示已投过同一个 messageId；轮次已经推进，这条 turn 过期了

把 probeInFlight 设为 false(tryAcquire 占了探针位，必须还回去，不然探针位永久卡住)

```java
public void onSettled(String agentId) {
    if (agentId == null) {
        return;
    }
    Health h = health.get(agentId);
    if (h == null) {
        return;
    }
    synchronized (h) {
        h.probeInFlight = false;
    }
}
```

## 设计细节

### 熔断的完整状态机

#### 这个项目的熔断有哪几个状态？从正常到打开，中间经历什么？

闭合状态(一开始 openUntil = 0)连续失败 N 次后变为打开(openUntil > now)，等 circuitOpenMs 时间过了进入半开状态(openUntil<now && !=0)，放一个探针出去，探针成功回到闭合状态，探针失败回到打开状态

##### 连续失败几次触发熔断？这个数怎么定的？定大了会怎样？定小了会怎样？

3次，定太小了会导致 "偶发抖动就熔断 —— 网络抖一下、对方刚好在重启，就被断了，误伤正常 peer"，定太大了会导致 "白等更多时间"

协商场景里，每个 peer 每轮最多收 1 次投递（一轮就是一次 SOLICIT），所以阈值不能太大 —— 太大意味着要坏好几轮才熔断，整场协商都被拖死了。所以倾向于小阈值+短打开窗口，因为协商是低频、短时长的场景，不值得等太久。

没有使用滑动窗口失败率（最近 N 次调用里，失败的比例超过 X%，就熔断），因为协商场景每个 peer 一轮就一次调用，连续坏 3 轮基本就是真挂了。如果是高并发网关那种场景，确实应该用滑动窗口失败率，但我们这个场景简单够用。

##### 熔断打开之后，过多久会尝试恢复？这个恢复机制是怎样的？

30 s，机制：不是主动定时去试探而是被动等待，openUntil 到期后，下一次有请求进来时，发现 openUntil <= now，就自动进入半开状态，放一个探针出去。

##### 恢复的时候，你是放一个请求出去试试，还是放多个？为什么？如果放一个，这个请求失败了怎么办？成功了怎么办？

放一个，

探针失败：直接重新打开熔断，openUntil = now + circuitOpenMs

探针成功：重置——`failures = 0`、`openUntil = 0`、`probeInFlight = false`，回到闭合状态

##### 间歇性故障怎么处理

间歇性故障在协商场景里可以接受：
