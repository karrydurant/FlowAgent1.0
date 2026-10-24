# Agent 注册与发现

# AgentRegistryService

Agent 注册中心服务

负责 Agent 的注册、发现、健康检查和能力匹配

Agent 启动时向注册中心注册自己的 Agent Card，其它组件通过注册中心发现可用的 Agent

核心功能：

注册——Agent 启动时上报 Agent Card

发现——按能力标签（精确匹配 + 通配）或场景子串查询可用的 Agent

心跳——定期续约并检测存活状态

Agent 注册中心的查询维度是「能力」而非服务名

实现边界：真正的语义匹配还未实现

```java
package com.flowagent.registry.discovery;

import com.alibaba.fastjson2.JSON;
import com.flowagent.common.model.AgentCard;
import com.flowagent.common.constant.CacheConstants;
import com.flowagent.common.exception.FlowAgentException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class AgentRegistryService {
    private final RedissonClient redissonClient;

    /**
     * 心跳超时（秒）—— 超过这个时间没有心跳即视为下线。
     * 这个值同时用作 Redis key 的 TTL，两者必须一致
     */
    @Value("${flowagent.registry.heartbeat-timeout-seconds:90}")
    private long heartbeatTimeoutSeconds = 90L;

    // 内存缓存：agentId->AgentCard
    private final Map<String, AgentCard> agentCache = new ConcurrentHashMap<>();

    // 能力索引：capability → agentId 列表
    private final Map<String, Set<String>> capabilityIndex = new ConcurrentHashMap<>();

    private Duration cardTtl() {
        return Duration.ofSeconds(heartbeatTimeoutSeconds);
    }

    private long heartbeatTimeoutMillis() {
        return heartbeatTimeoutSeconds * 1000L;
    }

    // 心跳超时配置（秒），供健康检查/运维接口展示。
    public long getHeartbeatTimeoutSeconds() {
        return heartbeatTimeoutSeconds;
    }

    // 注册 Agent
    public void register(AgentCard card) {
        if (card == null || card.getAgentId() == null || card.getAgentId().isBlank()) {
            throw FlowAgentException.invalidParam("Agent Card 必须包含 agentId");
        }
        String agentId = card.getAgentId();

        // 重复注册时先清掉旧的能力索引，否则改过 capabilities 后旧标签会残留
        AgentCard previous = agentCache.get(agentId);
        if (previous != null) {
            removeFromCapabilityIndex(previous);
            warnIfEndpointMoved(previous, card);
        }

        long now = System.currentTimeMillis();
        if (card.getRegisteredAt() == null) {
            card.setRegisteredAt(now);
        }
        card.setLastHeartbeatAt(now);
        
        persist(card);

        agentCache.put(agentId, card);

        addToCapabilityIndex(card);

        log.info("[Registry] agent registered | id={} | name={} | capabilities={}",
                agentId, card.getName(), card.getCapabilities());
    }

    // 同一个 agentId 的 a2aEndpoint 被改成了另一个地址时告警一次
    private void warnIfEndpointMoved(AgentCard previous, AgentCard current) {
        String before = previous.getA2aEndpoint();
        String now = current.getA2aEndpoint();
        if (before == null || now == null || before.equals(now)) {
            return;
        }

        // 按 (agentId, 新地址) 去重
        if (!warnedEndpointMoves.add(current.getAgentId() + '|' + now)) {
            return;
        }
        log.warn("[Registry] agentId {} 的 a2aEndpoint 被改写了：{} → {}。"
                        + "注册表没有实例维度，两个副本发布同名 Agent 时地址会互相覆盖，"
                        + "已开会话的投递会跟着换进程。",
                current.getAgentId(), before, now);
    }
}
```

## 如何发现对应的 Agent

按能力标签（精确匹配+通配）
