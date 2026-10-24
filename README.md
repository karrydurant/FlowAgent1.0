# FlowAgent1.0

<p>
  <img alt="Java" src="https://img.shields.io/badge/Java-17-orange?logo=openjdk&logoColor=white">
  <img alt="Spring Boot" src="https://img.shields.io/badge/Spring%20Boot-3.2.5-6DB33F?logo=springboot&logoColor=white">
  <img alt="Spring AI" src="https://img.shields.io/badge/Spring%20AI-1.0.0-6DB33F?logo=spring&logoColor=white">
  <img alt="Maven" src="https://img.shields.io/badge/Maven-3.9-C71A36?logo=apachemaven&logoColor=white">
  <img alt="Docker" src="https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white">
</p>
<p>
  <img alt="Redis" src="https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white">
  <img alt="Neo4j" src="https://img.shields.io/badge/Neo4j-5-008CC1?logo=neo4j&logoColor=white">
  <img alt="PostgreSQL" src="https://img.shields.io/badge/PostgreSQL-16%20+%20pgvector-336791?logo=postgresql&logoColor=white">
  <img alt="MySQL" src="https://img.shields.io/badge/MySQL-8-4479A1?logo=mysql&logoColor=white">
</p>


这是一个用 Java 和 Spring AI 写的多 Agent 工作流编排平台，把多个 Agent "怎么串成流程，怎么'协商'、互相委派" 做成了一套基础设施。

协商：这里的协商是 "召集者把同一个议题分发给每个成员，各成员生成结构化表态后再按模式算结论"，不是"多个 Agent 间互相聊天、辩论" 

互相委派：通过将一个 Agent 注册为工具的方式使得其它 Agent 能委派该 Agent

## 使用指南

前置依赖检查

```bash
java -version
mvn -v
docker compose version
node -v
```

终端一

```bash
# 1. 拉取代码
git clone <仓库地址> flowagent
cd flowagent

# 2. 设置环境变量（替换成你自己的令牌和配置）
export OPENAI_API_KEY=ms-你的魔搭令牌
export ROCKETMQ_NAME_SERVER=127.0.0.1:9876
export ROCKETMQ_PRODUCER_GROUP=flowagent-producer

# 3. 启动所有依赖中间件：redis neo4j postgres mysql rocketmq
docker compose up -d --wait redis neo4j postgres mysql rocketmq-namesrv rocketmq-broker

# 4. 循环等待 RocketMQ broker 启动成功（最多等待60秒）
for i in $(seq 1 30); do
  docker logs flowagent-rocketmq-broker 2>&1 | grep -q 'boot success' && break
  sleep 2
done

# 校验 namesrv、broker 是否启动成功
docker logs flowagent-rocketmq-namesrv --tail 5 | grep 'boot success'
docker logs flowagent-rocketmq-broker --tail 5 | grep 'boot success'

# 5. 导入 MySQL 业务表结构
docker exec -i flowagent-mysql mysql -uroot -pchangeme < flowagent-server/src/main/resources/db/business-schema.sql
# 检查表是否导入成功
docker exec flowagent-mysql mysql -uroot -pchangeme -e "show tables from flowagent_business;"

# 6. 前端打包编译
cd flowagent-web
npm ci
npm run deploy
cd ..

# 7. maven 打包（跳过测试），然后启动后端服务，日志输出到 flowagent.log
mvn -DskipTests clean install
mvn spring-boot:run -pl flowagent-server 2>&1 | tee flowagent.log
```

终端2

```bash
cd flowagent

# 轮询等待后端健康端口就绪
until curl -sf http://localhost:8081/actuator/health >/dev/null; do sleep 2; done

# 打印健康状态
curl -s http://localhost:8081/actuator/health; echo

# 查看日志里是否输出 [Messaging] 审批唤醒（确认消息组件正常）
grep -m1 '\[Messaging\] 审批唤醒' flowagent.log

# 调用 MCP tools/list 接口测试
curl -s -X POST http://localhost:8081/mcp \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'; echo
```

```plaintext
# 主页面
http://localhost:8081
# Swagger 接口文档
http://localhost:8081/swagger-ui.html
```

## [多 Agent 工作流的执行引擎](docs/workflow-engine/workflow-engine.md)

## [多 Agent 协商引擎](docs/a2a-engine/a2a-engine.md)

## [A2A 会话与消息传输](docs/a2a-conversation/a2a-conversation.md)

## [ReAct 循环](docs/ReAct/ReAct.md)

## [MCP 工具网关](docs/mcp-gateway/mcp-gateway.md)

## [人工审批与挂起恢复](docs/human-approve/human-approve.md)

## [Agent 注册与能力发现](docs/agent-registry/agent-registry.md)
