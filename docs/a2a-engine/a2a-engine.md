# 多 Agent 协商引擎

协商引擎解决的是：进程崩溃、副本切换、Agent 宕机、网络抖动、并发洪峰场景下，结果还是对的

作用主要有：

1.组织多轮表态：把议题发给多个 Agent，收齐各自意见；下一轮可以把别人的意见带上，让它们改口或坚持。

2.按规则计票：支持多数、加权、一票否决、法定人数、弃权。

3.管住时间和失败：整场一个总截止时间，慢成员不会拖死全局；某个成员失败或超时，按规则处理，而不是让整场卡住。

4.保证过程可靠：协商状态落盘，协调者崩了能被接手续跑；重复投递不会让同一个 Agent 跑两遍；最后留下完整记录，能说清谁在第几轮说了什么、谁缺席、结论怎么来的。

## 状态字段——NegotiationState

## 业务流程——NegotiationCoordinator

### 轮次状态机——NegotiationRoundRunner

### 投递执行器——NegotiationDelivery



## 多副本安全的实现——NegotiationStateStore的transition方法

```java

```

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
