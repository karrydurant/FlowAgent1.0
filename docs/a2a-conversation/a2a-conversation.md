# 多 Agent 会话与消息传输

## 会话管理器——SessionManager

负责开会话、投递消息、读取、收尾

### persist

把会话和消息写进 Redis。

工作流程：校验、去重、预占序号、落盘、合并元数据。

```java

```

## 会话存储——SessionStore



## 对端分发器——PeerDispatcher

## 委派管理——DelegationManager
