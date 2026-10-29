# 多 Agent 会话与消息传输

## 会话管理器——SessionManager

负责开会话、投递消息、读取、收尾

### persist

把会话和消息写进 Redis。

工作流程：校验、去重、预占序号、落盘、合并元数据。

```java
private A2aSession persist(String sessionId, A2aMessage message) {
    // 消息体不能为空
    if (message == null) {
        throw FlowAgentException.invalidParam("消息体不能为空");
    }
    A2aSession session = requireOpenSession(sessionId);

    String from = message.getFromAgentId();
    // 发送方不能为空
    if (from == null || from.isBlank()) {
        throw FlowAgentException.invalidParam("发送方 fromAgentId 不能为空");
    }
    // 发送方必须在会话成员里
    if (!session.hasMember(from)) {
        throw FlowAgentException.of("NOT_A_MEMBER",
                "发送方不在会话成员里: " + from, 403);
    }
    String to = message.getToAgentId();
    if (to != null) {
        // 自环检查
        // 正常链路不会自转，这个检查是防御性的 —— 配置错了或者代码有 bug 时，及时拦下
        if (to.equals(from)) {
            throw FlowAgentException.of("SESSION_LOOP",
                    "接收方就是发送方自身，会形成自环: " + from, 409);
        }
        // 接收方必须在会话成员里
        if (!session.hasMember(to)) {
            throw FlowAgentException.of("NOT_A_MEMBER",
                    "接收方不在会话成员里: " + to, 403);
        }
    }

    fillDefaults(message, sessionId);

    // replyTo 与 fromAgentId 都是请求体自填的
    // 没有这道检查，B 可以提交一条 replyTo 指向「发给 A 的请求」的回复，在会话日志里冒充 A 的表态。
    if (message.getReplyTo() != null && !message.getReplyTo().isBlank()) {
        try {
            store.findMessage(sessionId, message.getReplyTo()).ifPresent(original -> {
                String expected = original.getToAgentId();
                if (expected != null && !expected.isBlank() && !expected.equals(from)) {
                    throw FlowAgentException.of("REPLY_ATTRIBUTION_MISMATCH",
                            "回复方 " + from + " 不是原消息的接收方 " + expected, 403);
                }
            });
        } catch (FlowAgentException e) {
            throw e;
        } catch (RuntimeException e) {
            // fail-open：读不到原消息不能变成「回复写不进去」
            log.warn("[A2A] 读取被回复消息失败，跳过归属校验 | session={} | replyTo={} | error={}",
                    sessionId, message.getReplyTo(), e.toString());
        }
    }

    // 这条消息要不要做去重保护
    // 只有回复类消息要做去重保护（因为同一个问题应该只有一个答案）
    boolean guardable = message.getReplyTo() != null && !message.getReplyTo().isBlank()
            && REPLY_DEDUPE_KINDS.contains(message.getKind());
    // 有没有抢到幂等槽
    boolean claimed = false;
    if (guardable) {
        try {
            // 抢锁
            A2aMessage winner = store.claimReply(message);
            // 抢锁失败返回赢家信息
            if (winner != null) {
                // 不落盘、不投递，把先到的那条原样返回
                // 调用方拿到的仍然是一个带 messageId 与 seq 的真实消息，只是它不是它刚提交的那一份
                long n = replyDuplicatesSkipped.incrementAndGet();
                log.warn("[A2A] 重复回复已拦下，返回先到的那条 | session={} | from={} | replyTo={} | 累计 {} 条",
                        sessionId, message.getFromAgentId(), message.getReplyTo(), n);
                throw new DuplicateReply(winner);
            }
            claimed = true;
        } catch (DuplicateReply duplicate) {
            // 重复回复异常
            throw duplicate;
        } catch (Exception e) {
            // fail-open 降级——Redis 抖动时，锁抢不到，系统不阻塞写入，直接放行
            long n = replyGuardFailures.incrementAndGet();
            // 日志留痕
            if (n == 1 || n % 100 == 0) {
                // 计数逐次，日志限流
                log.warn("[A2A] 回复去重闸读不到，本次放行不做去重 | 累计 {} 次 | session={} | replyTo={} | error={}",
                        n, sessionId, message.getReplyTo(), e.toString());
            }
        }
    }

    long reserved = -1L;
    // seq>0 说明已经预置过序号了不过 append 还没有执行进程就崩了
    if (message.getSeq() <= 0) {
        // 预占
        reserved = store.reserveSeq(sessionId, messageBudget);
        // 返回 -1，说明超过消息预算
        if (reserved < 0) {
            if (claimed) {
                // 释放锁
                releaseQuietly(message);
            }
            throw FlowAgentException.of("MESSAGE_BUDGET_EXCEEDED",
                    "会话 " + sessionId + " 已达消息预算 " + messageBudget + " 条", 429);
        }
        // 给内存里的java对象 setSeq
        message.setSeq(reserved);
    }

    long seq;
    try {
        seq = store.append(message);
    } catch (RuntimeException e) {
        // 为什么 try 只包住 append
        // 消息一旦写进 Redis，就算后续唤醒对端失败，也不能回滚序号和锁
        // 因为消息已经在日志里了，回滚了就等于允许再写一遍，会造成重复
        if (reserved > 0) {
            // 预占了序号要回滚
            rollbackQuietly(sessionId, reserved);
        }
        if (claimed) {
            releaseQuietly(message);
        }
        throw e;
    }
    if (claimed) {
        // 给 Redis 上存的信息补上 seq
        refreshQuietly(message);
    }
    
    signalReply(sessionId, message);
    session.setMessageCount((int) seq);
    session.setUpdatedAt(Instant.now());
    store.updateAfterAppend(session);
    return session;
}
```

备注：**Redis 是跨进程的，它存不了 Java 对象**

## 会话存储——SessionStore



## 对端分发器——PeerDispatcher

## 委派管理——DelegationManager
