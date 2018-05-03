---
title: SparkRpcCore-MasterImplement
date: 2018-05-02 22:35:00
tags: spark-internal
---

# Spark RPC 核心架构实现

我们都知道 Spark 在 2.x 以后，使用 Netty 作为 Spark RPC 运行的基础框架，所以实现时和核心架构略有不同。

{% note info %}
Spark RPC 实现时分为两种模式：本地模式（ Local Mode ）和 远程模式（ Remote Mode ）。
由于两种模式存在差别，所以分开说明。
{% endnote %}

# 本地模式 ( Local Mode )

{% note info %}
举一个 Spark Standlone 模式下 Master 启动时使用 RPC 获取 MasterWebUIPort 和 MasterRestServerPort 为例
说明 本地模式（ Local Mode ）下 Spark RPC 是如何完成工作的
{% endnote %}

<!-- more -->

# 源码

## 初始化 NettyRpcEnv

### main()

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

### startRpcEnvAndEndpoint()

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

## 初始化和注册 Endpoint

### rpcEnv.setupEndpoint()

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

## 发送消息

{% note danger %}
写消息队列的过程
{% endnote %}

### RpcEndpointRef().askSync()

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
  val promise = Promise[Any]()
  // NettyRpcEndpointRef.address => Endpoint.address
  val remoteAddr = message.receiver.address
  // 失败执行函数
  def onFailure(e: Throwable): Unit = {
    if (!promise.tryFailure(e)) {
      e match {
        case e : RpcEnvStoppedException => logDebug (s"Ignored failure: $e")
        case _ => logWarning(s"Ignored failure: $e")
      }
    }
  }
  // 成功执行函数
  def onSuccess(reply: Any): Unit = reply match {
    case RpcFailure(e) => onFailure(e)
    case rpcReply =>
      if (!promise.trySuccess(rpcReply)) {
        logWarning(s"Ignored message: $reply")
      }
  }

  try {
    // 如果 Endpoint.address 等于 NettyRpcEnv.address
    if (remoteAddr == address) {
      val p = Promise[Any]()
      p.future.onComplete {
        case Success(response) => onSuccess(response) // 注册成功执行函数
        case Failure(e) => onFailure(e)               // 注册失败执行函数
      }(ThreadUtils.sameThread)
      dispatcher.postLocalMessage(message, p)
    } else {
      val rpcMessage = RpcOutboxMessage(message.serialize(this),
        onFailure,
        (client, response) => onSuccess(deserialize[Any](client, response)))
      postToOutbox(message.receiver, rpcMessage)
      promise.future.failed.foreach {
        case _: TimeoutException => rpcMessage.onTimeout()
        case _ =>
      }(ThreadUtils.sameThread)
    }
  [...]
}
{% endcodeblock %}

#### postLocalMessage && postMessage()

{% codeblock lang:scala postLocalMessage https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/Dispatcher.scala Dispatcher.scala %}
def postLocalMessage(message: RequestMessage, p: Promise[Any]): Unit = {
  val rpcCallContext = new LocalNettyRpcCallContext(message.senderAddress, p)
  // 将 RequestMessage 转换成为 RpcMessage
  val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
  // 发送一个 Rpc 消息
  // message.receiver.name => NettyRpcEndpointRef.name => RpcEndpointAddress.name => Endpoint.name
  postMessage(message.receiver.name, rpcMessage, (e) => p.tryFailure(e))
}
{% endcodeblock %}

{% codeblock lang:scala postMessage https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/Dispatcher.scala Dispatcher.scala %}
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

{% note danger %}
至此已经完成了 Master.main() 函数中 Rpc 所有写（发送）消息的过程。
{% endnote %}

## 处理消息

{% note danger %}
读消息队列的过程
{% endnote %}

### Dispatcher.threadpool

{% note info %}
该线程池是在初始化 Dispatcher 类时完成初始化， Dispatcher 类初始化是在初始化 NettyRpcEnv 时初始化
也就是说在初始化 NettyRpcEnv 时，该线程池已经初始化完成
{% endnote %}

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/Dispatcher.scala Dispatcher.scala %}
// 该线程池是在初始化 Dispatcher 类时完成初始化， Dispatcher 类初始化是在初始化 NettyRpcEnv 时初始化
// 也就是说在初始化 NettyRpcEnv 时，该线程池已经初始化完成
private val threadpool: ThreadPoolExecutor = {
  // 获取所有可用的核
  val availableCores = if (numUsableCores > 0) numUsableCores else Runtime.getRuntime.availableProcessors()
  // 根据核获取可用的线程数
  val numThreads = nettyEnv.conf.getInt("spark.rpc.netty.dispatcher.numThreads", math.max(2, availableCores))
  // 初始化一个 "dispatcher-event-loop" 线程池
  val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop")
  // 启动线程池中的每一个线程，执行 new MessageLoop
  for (i <\- 0 until numThreads) {
    pool.execute(new MessageLoop)
  }
  // 返回线程池
  pool
}
{% endcodeblock %}

### Dispatcher.MessageLoop()

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/Dispatcher.scala Dispatcher.scala %}
// 死循环执行 receivers.take() 和 data.inbox.process(Dispatcher.this)
private class MessageLoop extends Runnable {
  override def run(): Unit = {
    try {
      while (true) {
        try {
          // 从待处理消息队列中取出一个待处理的消息
          val data = receivers.take()
          if (data == PoisonPill) {
            // Put PoisonPill back so that other MessageLoops can see it.
            receivers.offer(PoisonPill)
            return
          }
          // 处理消息
          data.inbox.process(Dispatcher.this)
        } catch {
          case NonFatal(e) => logError(e.getMessage, e)
        }
      }
    } catch {
      case ie: InterruptedException => // exit
    }
  }
}
{% endcodeblock %}

#### Inbox.process()

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/Inbox.scala Inbox.scala %}
// 实际处理消息函数
def process(dispatcher: Dispatcher): Unit = {
  var message: InboxMessage = null
  inbox.synchronized {
    // 从 inbox.messages 从取出头元素，FIFO
    message = messages.poll()
  }
  while (true) {
    safelyCall(endpoint) {
      // 根据不同的消息类型调用 Endpoint 不同的方法
      message match {
        case RpcMessage(_sender, content, context) =>
          endpoint.receiveAndReply(context).applyOrElse[Any, Unit](content, { msg =>
            throw new SparkException(s"Unsupported message $message from ${_sender}")
          })
        case OneWayMessage(_sender, content) =>
          endpoint.receive.applyOrElse[Any, Unit](content, { msg =>
            throw new SparkException(s"Unsupported message $message from ${_sender}")
          })
        case OnStart =>
          endpoint.onStart()
        case OnStop =>
          dispatcher.removeRpcEndpointRef(endpoint)
          endpoint.onStop()
        case RemoteProcessConnected(remoteAddress) =>
          endpoint.onConnected(remoteAddress)
        case RemoteProcessDisconnected(remoteAddress) =>
          endpoint.onDisconnected(remoteAddress)
        case RemoteProcessConnectionError(cause, remoteAddress) =>
          endpoint.onNetworkError(cause, remoteAddress)
      }
    }
  }
}
{% endcodeblock %}

{% note danger %}
让我们看一下处理现有队列中存在消息（ OnStart, BoundPortsResponse ）的实现
OnStart 是 OnStart 类，所以调用 endpoint.OnStart() 方法
BoundPortsResponse 是 RpcMessage 类，所以调用 endpoint.receiveAndReply() 方法
{% endnote %}

### Master.OnStart()

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala Master.scala %}
override def onStart(): Unit = {
  // 启动 MasterWebUI
  webUi = new MasterWebUI(this, webUiPort)
  webUi.bind()
  // 启动 RestServer
  if (restServerEnabled) {
    val port = conf.getInt("spark.master.rest.port", 6066)
    restServer = Some(new StandaloneRestServer(address.host, port, conf, self, masterUrl))
  }
  restServerBoundPort = restServer.map(_.start())
  // 启动 MetricsSystem
  masterMetricsSystem.registerSource(masterSource)
  masterMetricsSystem.start()
  applicationMetricsSystem.start()
  // 设置 Master 热备模式
  // 默认是 (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this)) 意思是不备份，无法恢复
  val (persistenceEngine_, leaderElectionAgent_) = RECOVERY_MODE match {
    case "ZOOKEEPER" =>
      logInfo("Persisting recovery state to ZooKeeper")
      val zkFactory =
        new ZooKeeperRecoveryModeFactory(conf, serializer)
      (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
    case "FILESYSTEM" =>
      val fsFactory =
        new FileSystemRecoveryModeFactory(conf, serializer)
      (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))
    case "CUSTOM" =>
      val clazz = Utils.classForName(conf.get("spark.deploy.recoveryMode.factory"))
      val factory = clazz.getConstructor(classOf[SparkConf], classOf[Serializer])
        .newInstance(conf, serializer)
        .asInstanceOf[StandaloneRecoveryModeFactory]
      (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))
    case _ =>
      (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
  }
  persistenceEngine = persistenceEngine_
  leaderElectionAgent = leaderElectionAgent_
}
{% endcodeblock %}

### Master.receiveAndReply()

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala Master.scala %}
// receiveAndReply 会根据不同的类型，做不同的处理
// 由于本次传过来的是 BoundPortsResponse 所以我们这里只关注 BoundPortsResponse 处理方法即可
override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
  [...]
  case BoundPortsRequest =>
    context.reply(BoundPortsResponse(address.port, webUi.boundPort, restServerBoundPort))
  [...]
}
{% endcodeblock %}

#### LocalNettyRpcCallContext.reply() && NettyRpcCallContext.reply()

{% note info %}
由于是本地模式（ Local ）所以初始化 RpcMessage 的时候使用的是 LocalNettyRpcCallContext
`val rpcCallContext = new LocalNettyRpcCallContext(message.senderAddress, p)`
所以这里应该是调用 LocalNettyRpcCallContext.reply()
{% endnote %}

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcCallContext.scala NettyRpcCallContext.scala %}
// 由于 LocalNettyRpcCallContext 没有 reply 方法，所以调用父类 NettyRpcCallContext 的 replay 方法
// 由于父类 NettyRpcCallContext 的 send 方法为抽象方法，所以要调用子类 LocalNettyRpcCallContext 的 send 方法
private[netty] abstract class NettyRpcCallContext(override val senderAddress: RpcAddress)
  extends RpcCallContext with Logging {
  // 抽象
  protected def send(message: Any): Unit
  override def reply(response: Any): Unit = {
    // 调用 send 方法
    send(response)
  }
  override def sendFailure(e: Throwable): Unit = {
    send(RpcFailure(e))
  }
}
{% endcodeblock %}

{% note danger %}
由于 LocalNettyRpcCallContext 没有 reply 方法，所以调用父类 NettyRpcCallContext 的 replay 方法
由于父类 NettyRpcCallContext 的 send 方法为抽象方法，所以要调用子类 LocalNettyRpcCallContext 的 send 方法
{% endnote %}

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcCallContext.scala NettyRpcCallContext.scala %}
private[netty] class LocalNettyRpcCallContext(
    senderAddress: RpcAddress,
    p: Promise[Any])
  extends NettyRpcCallContext(senderAddress) {
  // 最后调用 send 方法，执行 Promise.onSuccess 函数（在 NettyRpcEnv.ask 方法中注册过 Promise 的 onSuccess 函数）
  override protected def send(message: Any): Unit = {
    p.success(message)
  }
}
{% endcodeblock %}

`-EOF-`
