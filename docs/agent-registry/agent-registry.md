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

# AgentCard

Agent 的数字名片。

核心字段：

签名——JWS 确保不可伪造

能力描述——自然语言 + 结构化标签，供 LLM 路由决策

端点——A2A 接口 URL + 健康检查 URL

安全策略——支持的认证方式、所需权限

```java
package com.flowagent.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * A2A Agent Card — Agent 的"数字名片"。
 * <p>
 * 符合 Google A2A 协议 v1.0 规范。
 * 每个 Agent 在注册时签发一张 Agent Card，包含身份、能力、
 * 端点、安全策略等元数据。其他 Agent 通过 Agent Card 发现
 * 和验证对方身份。
 * </p>
 *
 * <p>
 * <b>模块归属说明</b>：本类同时被 A2A 网关（签发/使用）与
 * Agent 注册中心（存储/查询）引用。为避免两者形成循环依赖，
 * 将其下沉到 {@code flowagent-common} 作为共享领域模型。
 * </p>
 *
 * <h3>Agent Card 的核心字段</h3>
 * <ul>
 *   <li><b>签名</b> — JWS (JSON Web Signature) 确保不可伪造</li>
 *   <li><b>能力描述</b> — 自然语言 + 结构化标签，供 LLM 路由决策</li>
 *   <li><b>端点</b> — A2A 接口 URL + 健康检查 URL</li>
 *   <li><b>安全策略</b> — 支持的认证方式、所需权限</li>
 * </ul>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AgentCard {

    // ======================== 基础信息 ========================

    /** Agent 唯一标识（全局唯一） */
    private String agentId;

    /** Agent 显示名称 */
    private String name;

    /** Agent 版本 */
    @Builder.Default
    private String version = "1.0.0";

    /** Agent 描述（自然语言，供 LLM 路由判断） */
    private String description;

    /** 所属组织 */
    private String organization;

    // ======================== 能力描述 ========================

    /** 能力标签（结构化，用于精确匹配） */
    private List<String> capabilities;

    /** 专长领域（自然语言描述；当前不参与检索，语义匹配是 Roadmap 项） */
    private String expertise;

    /** 适用场景 */
    private List<String> applicableScenarios;

    /** 限制条件（什么情况下不应该调用此 Agent） */
    private String limitations;

    // ======================== 端点 ========================

    /** A2A 协议端点 URL */
    private String a2aEndpoint;

    /** 健康检查端点 */
    private String healthCheckEndpoint;

    /** MCP 端点（此 Agent 暴露的工具） */
    private String mcpEndpoint;

    // ======================== 安全 ========================

    /** Agent Card 签名（JWS） */
    private String signature;

    /** 签名公钥（JWK 格式） */
    private Map<String, Object> publicKey;

    /** 支持的认证方式 */
    @Builder.Default
    private List<String> authMethods = List.of("bearer_token", "api_key");

    /** 所需的最小权限级别 */
    @Builder.Default
    private String requiredPermissionLevel = "read";

    // ======================== 运行时配置 ========================

    /** 默认 LLM 模型 */
    private String defaultModel;

    /** 支持的工具 ID 列表 */
    private List<String> supportedToolIds;

    /** 预期的最大响应时间（秒） */
    private int expectedMaxResponseTimeSeconds;

    /** 是否接受委派任务 */
    @Builder.Default
    private boolean acceptsDelegation = true;

    /** 并发任务上限 */
    @Builder.Default
    private int maxConcurrentTasks = 5;

    // ======================== 元数据 ========================

    /** 自定义标签 */
    private Map<String, String> labels;

    /** 注册时间 */
    private Long registeredAt;

    /** 最后心跳时间 */
    private Long lastHeartbeatAt;
}
```
