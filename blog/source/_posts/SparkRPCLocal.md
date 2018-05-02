---
title: Spark RPC
date: 2018-04-25 23:56:21
tags: spark-internal
---

# Spark RPC 核心架构

  Spark RPC 的核心逻辑架构有部分组成，分别是：`RpcEnv`，`RpcEndpointRef` 和 `RpcEndpoint`。

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

# Spark RPC 核心架构实现

我们都知道 Spark 在 2.x 以后，使用 Netty 作为 Spark RPC 运行的基础框架，所以实现时和核心架构略有不同。

{% note info %}
Spark RPC 实现时分为两种模式：本地模式（Local Mode）和 远程模式（Remote Mode）。由于两种模式存在差别，所以分开说明。
{% endnote %}

## Spark RPC (Local Mode)

### Master.main()

#### main()

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala Master.scala %}
// Master 主函数
def main(argStrings: Array[String]) {
  [...]
  val conf = new SparkConf
  val args = new MasterArguments(argStrings, conf)
  // 调用 startRpcEnvAndEndpoint 方法初始化 Master 的 Endpoint
  val (rpcEnv, _, _) = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, conf)
  // 同步等待
  rpcEnv.awaitTermination()
}
{% endcodeblock %}

#### startRpcEnvAndEndpoint()

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala Master.scala %}
val SYSTEM_NAME = "sparkMaster"
val ENDPOINT_NAME = "Master"

def startRpcEnvAndEndpoint(
    host: String,
    port: Int,
    webUiPort: Int,
    conf: SparkConf): (RpcEnv, Int, Option[Int]) = {
  [...]
  // 创建 NettyRpcEnv
  val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
  val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
                                            new Master(rpcEnv,
                                                       rpcEnv.address,
                                                       webUiPort,
                                                       securityMgr,
                                                       conf))
  val portsResponse = masterEndpoint.askSync[BoundPortsResponse](BoundPortsRequest)

  (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
}
{% endcodeblock %}

#### RpcEnv.create()

{% note info %}
这里扩展看一下 RpcEnv.create() 的过程。
{% endnote %}

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/RpcEnv.scala RpcEnv.scala %}
def create(
    name: String,
    bindAddress: String, // 默认是 host
    advertiseAddress: String, // 默认是 bindAddress
    port: Int,
    conf: SparkConf,
    securityManager: SecurityManager,
    numUsableCores: Int, // 默认为 0
    clientMode: Boolean): RpcEnv = { // 默认为 false
  val config = RpcEnvConfig(conf, name, bindAddress, advertiseAddress, port, securityManager, numUsableCores, clientMode)
  new NettyRpcEnvFactory().create(config)
}
{% endcodeblock %}

#### NettyRpcEnvFactory().create()

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala NettyRpcEnv.scala %}
private[rpc] class NettyRpcEnvFactory extends RpcEnvFactory with Logging {
  // 创建 NettyRpcEnv
  def create(config: RpcEnvConfig): RpcEnv = {
    val sparkConf = config.conf
    // 创建 NettyRpcEnv
    val nettyEnv = new NettyRpcEnv(sparkConf,
                                   javaSerializerInstance,
                                   config.advertiseAddress,
                                   config.securityManager,
                                   config.numUsableCores)
    // 如果不是 clientMode 则启动一个服务
    if (!config.clientMode) {
      val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
        nettyEnv.startServer(config.bindAddress, actualPort)
        (nettyEnv, nettyEnv.address.port)
      }
      try {
        Utils.startServiceOnPort(config.port, startNettyRpcEnv, sparkConf, config.name)._1
      } catch {
        case NonFatal(e) =>
          nettyEnv.shutdown()
          throw e
      }
    }
    nettyEnv
  }
}
{% endcodeblock %}

{% note danger %}
Master 通过 RpcEnv.create() 方法，最终得到了 NettyRpcEnv 。
{% endnote %}

#### rpcEnv.setupEndpoint()

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala NettyRpcEnv.scala %}
override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
  // 通过 Dispatcher 类，注册 RpcEndpoint 。
  dispatcher.registerRpcEndpoint(name, endpoint)
}
{% endcodeblock %}

#### registerRpcEndpoint()

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/Dispatcher.scala Dispatcher.scala %}
// 这里先列出了 Dispatcher 类中 最重要 的几个变量。
// 1. 消息内容 EndpointData
private class EndpointData(val name: String, val endpoint: RpcEndpoint, val ref: NettyRpcEndpointRef) {
  val inbox = new Inbox(ref, endpoint)
}
// 2. Endpoint 和 EndpointData 对应关系
private val endpoints: ConcurrentMap[String, EndpointData] = new ConcurrentHashMap[String, EndpointData]
// 3. Endpoint 和 EndpontRef 对应关系
private val endpointRefs: ConcurrentMap[RpcEndpoint, RpcEndpointRef] = new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]
// 4. 需要处理消息的队列
private val receivers = new LinkedBlockingQueue[EndpointData]

def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef = {
  val addr = RpcEndpointAddress(nettyEnv.address, name)
  // 初始化一个和 Endpoint 对应的 EndpointRef ，之后会将它们的关系分别记录在 EndpointData 和 endpointRefs 中。
  val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
  synchronized { // 阻塞注册，互斥锁
    if (stopped) {
      throw new IllegalStateException("RpcEnv has been stopped")
    }
    // 写 endpoints
    // new EndpointData 时，会写入一个 OnStart 消息。
    if (endpoints.putIfAbsent(name, new EndpointData(name, endpoint, endpointRef)) != null) {
      throw new IllegalArgumentException(s"There is already an RpcEndpoint called $name")
    }
    val data = endpoints.get(name)
    // 写 endpointRefs
    endpointRefs.put(data.endpoint, data.ref)
    // 写 receivers
    receivers.offer(data)  // for the OnStart message
  }

  // 最后返回注册 Endpoint 对应的 EndpointRef 对象。
  endpointRef
}
{% endcodeblock %}

#### new EndpointData() && new Inbox()

{% codeblock lang:scala EndpointData https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/Dispatcher.scala Dispatcher.scala %}
// 1. 消息内容 EndpointData
private class EndpointData(val name: String, val endpoint: RpcEndpoint, val ref: NettyRpcEndpointRef) {
  // 会初始化一个 Inbox 类。
  val inbox = new Inbox(ref, endpoint)
}
{% endcodeblock %}

{% codeblock lang:scala Inbox https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/Inbox.scala Inbox.scala %}
private[netty] class Inbox(
    val endpointRef: NettyRpcEndpointRef,
    val endpoint: RpcEndpoint)
  extends Logging {
  // 真正存储 Rpc 消息的地方。
  @GuardedBy("this")
  protected val messages = new java.util.LinkedList[InboxMessage]()
  [...]
  // 初始化完成时，首先会放入一个 OnStart 的消息。
  inbox.synchronized {
    messages.add(OnStart)
  }
}
{% endcodeblock %}

{% note danger %}
此时在消息内容队列 Dispatcher.receivers 中已经存在一个 OnStart 的消息等待处理。
{% endnote %}

{% note danger %}
以上就是整个注册 Endpoint 的过程，需要注意几点：
1. 注册 Endpoint 返回 EndpointRef
2. 注册成功后，会写入一个 OnStart 的消息（任务）到 待处理消息队列（ Dispatcher.receivers ）中
{% endnote %}

#### RpcEndpointRef().askSync()

{% note danger %}
继续之前的程序，接下来会调用 askSync 方法，发送消息。
`val portsResponse = masterEndpoint.askSync[BoundPortsResponse](BoundPortsRequest)`
这里本来应该调用 **NettyRpcEndpointRef** 的 askSync 方法，但是 NettyRpcEndpointRef 没有该方法，所以调用其父类（ **RpcEndpointRef** ）的 askSync 方法。
{% endnote %}

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/RpcEndpointRef.scala RpcEndpointRef.scala %}
// 若只有一个参数，则使用默认的超时时间。
def askSync[T: ClassTag](message: Any): T = askSync(message, defaultAskTimeout)
// 会先调用异步 ask 方法，然后等待结果，从而实现同步响应。
def askSync[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
  val future = ask[T](message, timeout)
  timeout.awaitResult(future)
}
// 由于是一个抽象方法，所以去对应的子类中实现。
def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]
{% endcodeblock %}

{% note danger %}
调用 askSync 方法，实际最后是调用 ask 方法。
但是由于 ask 方法是抽象方法，所以要调用子类也就是（ NettyRpcEndpointRef ）中的 ask 方法。
{% endnote %}

#### NettyRpcEndpointRef().ask() && NettyRpcEnv().ask()

{% codeblock lang:scala NettyRpcEndpointRef.ask() https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala NettyRpcEnv.scala %}
private[netty] class NettyRpcEndpointRef(
    @transient private val conf: SparkConf,
    private val endpointAddress: RpcEndpointAddress,
    @transient @volatile private var nettyEnv: NettyRpcEnv) extends RpcEndpointRef(conf) {
    [...]
    override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
      // 实际是调用 NettyRpcEnv.ask() 这里将信息包了一层成为 RequestMessage(NettyEnv.address, NettyRpcEndpointRef, message)
      nettyEnv.ask(new RequestMessage(nettyEnv.address, this, message), timeout)
    }
    [...]
}
{% endcodeblock %}

{% codeblock lang:scala NettyRpcEnv.ask() https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala NettyRpcEnv.scala %}
private[netty] def ask[T: ClassTag](message: RequestMessage, timeout: RpcTimeout): Future[T] = {
  [...]
  // NettyRpcEndpointRef.address => Endpoint.address
  val remoteAddr = message.receiver.address
  // 如果 Endpoint.address 等于 NettyRpcEnv.address
  if (remoteAddr == address) { // 本地
    dispatcher.postLocalMessage(message, p)
  } else { // 远程
    val rpcMessage = RpcOutboxMessage(message.serialize(this),
                                      onFailure,
                                      (client, response) => onSuccess(deserialize[Any](client, response)))
    postToOutbox(message.receiver, rpcMessage)
  }
}
{% endcodeblock %}

#### postLocalMessage && postMessage()

{% codeblock lang:scala postLocalMessage https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/Dispatcher.scala Dispatcher.scala %}
def postLocalMessage(message: RequestMessage, p: Promise[Any]): Unit = {
  [...]
  // 将 RequestMessage 转换成为 RpcMessage
  val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
  // 发送一个 Rpc 消息
  // message.receiver.name => NettyRpcEndpointRef.name => RpcEndpointAddress.name => Endpoint.name
  postMessage(message.receiver.name, rpcMessage, (e) => p.tryFailure(e))
}
{% endcodeblock %}

{% codeblock lang:scala postLocalMessage https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/Dispatcher.scala Dispatcher.scala %}
private def postMessage(
    endpointName: String,
    message: InboxMessage,
    callbackIfStopped: (Exception) => Unit): Unit = {

  val error = synchronized {
    // data => EndpointData
    val data = endpoints.get(endpointName)
    // inbox.post() => inbox.messages.add()
    data.inbox.post(message)
    // 待处理消息队列中添加 message
    receivers.offer(data)
    None
  }
  [...]
}
{% endcodeblock %}

{% note danger %}
此时若消息队列未处理应存在两条消息：
1. OnStart ( `messages.add(OnStart)` )
2. BoundPortsResponse ( `masterEndpoint.askSync[BoundPortsResponse](BoundPortsRequest)` )
{% endnote %}


## Spark RPC (Remote Mode)

（未完待续）

`-EOF-`
