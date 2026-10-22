# SessionManager

## 1.职责定位

SessionManager 是 A2A 协作的会话中枢：开会话、投递消息、读取、收尾。它本身不执行 Agent、不聚合意见、不跑协商——
那些分别由 PeerDispatcher / NegotiationManager / DelegationManager 完成。SessionManager 只管 "消息如何进入共享 Redis、如何通知到接收方"。

依赖：

SessionStore：Redis 读写（元数据、消息日志、序号、收件箱、回复去重槽）

AgentRegistryService：开会话时校验成员是否在线、是否接受委派、取端点快照

PeerDispatcher：本地成员的执行派发通道

