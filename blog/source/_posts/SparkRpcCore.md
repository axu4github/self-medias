---
title: SparkRpcCore
date: 2018-04-25 23:56:21
tags: spark-internal
---

# Spark RPC 核心

  Spark RPC 的核心有以下三个部分组成，分别是：`RpcEnv`，`RpcEndpointRef` 和 `RpcEndpoint`。

  - RpcEnv：RpcEndpoint 和 RpcEndpointRef 的对应关系
  - RpcEndpointRef：发送消息
  - RpcEndpoint：接受并处理消息

## Spark RPC 核心逻辑

### 注册

![](/images/markdown-img-paste-20180429221035589.png)

1. 初始化 RpcEndpoint
2. 调用 RpcEnv 的 setupEndpoint() 方法注册 RpcEndpoint 并生成对应的 RpcEndpointRef
3. 返回 RpcEndpointRef

### 交互

![](/images/markdown-img-paste-20180426235917141.png)

1. RpcEndpointRef 发送消息
2. RpcEnv 通过 RpcEndpointRef 路由到对应的 RpcEndpoint
3. RpcEndpoint 根据消息类型处理消息，之后将处理结果返回给对应的 RpcEndpointRef

<!-- more -->

## 源码

### RpcEndpointRef

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/RpcEndpointRef.scala RpcEndpointRef.scala %}
private[spark] abstract class RpcEndpointRef(conf: SparkConf)
  extends Serializable with Logging {
  // 地址
  def address: RpcAddress
  // 名称
  def name: String
  // 发消息，无回复（Fire-and-forget semantics）
  def send(message: Any): Unit
  // 发信息，异步等待回复
  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]
  def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)
  // 发信息，同步等待回复
  def askSync[T: ClassTag](message: Any): T = askSync(message, defaultAskTimeout)
  def askSync[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
    val future = ask[T](message, timeout)
    timeout.awaitResult(future)
  }
}
{% endcodeblock %}

### RpcEnv

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/RpcEnv.scala RpcEnv.scala %}
private[spark] abstract class RpcEnv(conf: SparkConf) {
  // 根据 RpcEndpoint 获取 RpcEndpointRef
  private[rpc] def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef
  // 监听地址
  def address: RpcAddress
  // 注册 RpcEndpoint，通过 name 和 RpcEndpoint 创建 RpcEndpointRef
  //（创建 RpcEndpoint 和 RpcEndpointRef 对应关系）
  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef
  // 通过 uri 异步创建 RpcEndpointRef
  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]
  // 通过 uri 同步创建 RpcEndpointRef
  def setupEndpointRefByURI(uri: String): RpcEndpointRef = {
    defaultLookupTimeout.awaitResult(asyncSetupEndpointRefByURI(uri))
  }
  // 通过 监听地址 和 RpcEndpoint.name 创建 RpcEndpointRef
  def setupEndpointRef(address: RpcAddress, endpointName: String): RpcEndpointRef = {
    setupEndpointRefByURI(RpcEndpointAddress(address, endpointName).toString)
  }
  // 同步停止
  def stop(endpoint: RpcEndpointRef): Unit
  // 异步停止
  def shutdown(): Unit
}
{% endcodeblock %}

### RpcEndpoint

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/RpcEndpoint.scala RpcEndpoint.scala %}
private[spark] trait RpcEndpoint {
  val rpcEnv: RpcEnv
  final def self: RpcEndpointRef = {
    require(rpcEnv != null, "rpcEnv has not been initialized")
    rpcEnv.endpointRef(this)
  }
  // 处理 RpcEndpointRef.send 和 RpcEndpointRef.reply 函数发送过来的消息
  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new SparkException(self + " does not implement 'receive'")
  }
  // 处理 RpcEndpointRef.ask 函数发送过来的消息
  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ => context.sendFailure(new SparkException(self + " won't reply anything"))
  }
}
{% endcodeblock %}

`-EOF-`
