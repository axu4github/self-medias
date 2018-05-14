---
title: SparkRpcCore-WorkerImplement
date: 2018-05-03 22:35:00
tags: spark-internal
---

# Spark RPC 核心架构实现

我们都知道 Spark 在 2.x 以后，使用 Netty 作为 Spark RPC 运行的基础框架，所以实现时和核心架构略有不同。

{% note info %}
Spark RPC 实现时分为两种模式：本地模式（ Local Mode ）和 远程模式（ Remote Mode ）。
由于两种模式存在差别，所以分开说明。
{% endnote %}

# 远程模式 ( Remote Mode )

{% note info %}
举一个 Spark Standlone 模式下 Worker 启动时使用 RPC 向 Master 注册为例
说明 远程模式（ Remote Mode ）下 Spark RPC 是如何完成工作的
{% endnote %}

<!-- more -->

# 源码

## 初始化 NettyRpcEnv

### main()

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala Worker.scala %}
// Worker 主函数
def main(argStrings: Array[String]) {
    [...]
    val rpcEnv = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, args.cores,
      args.memory, args.masters, args.workDir, conf = conf)
    [...]
    // 同步等待
    rpcEnv.awaitTermination()
}
{% endcodeblock %}

### startRpcEnvAndEndpoint()

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala Worker.scala %}
def startRpcEnvAndEndpoint(
    host: String,
    port: Int,
    webUiPort: Int,
    cores: Int,
    memory: Int,
    masterUrls: Array[String],
    workDir: String,
    workerNumber: Option[Int] = None,
    conf: SparkConf = new SparkConf): RpcEnv = {

  val systemName = SYSTEM_NAME + workerNumber.map(_.toString).getOrElse("")
  val securityMgr = new SecurityManager(conf)
  // 创建 NettyRpcEnv
  val rpcEnv = RpcEnv.create(systemName, host, port, conf, securityMgr)
  // 获取 Master 的地址
  val masterAddresses = masterUrls.map(RpcAddress.fromSparkURL(_))
  // 注册 Endpoint(Work)
  rpcEnv.setupEndpoint(ENDPOINT_NAME, new Worker(rpcEnv, webUiPort, cores, memory,
    masterAddresses, ENDPOINT_NAME, workDir, conf, securityMgr))
  rpcEnv
}
{% endcodeblock %}

{% note danger %}
如果看过 [SparkRpcCore-MasterImplement](http://localhost:4000/2018/05/02/SparkRpcCore-MasterImplement/) 的同学应该知道 rpcEnv.setupEndpoint() 其实是一个 RPC 的本地调用，最终会执行 Endpoint.OnStart() 方法
由于 SparkRpcCore-MasterImplement 已经描述过整个过程，所以这里不再赘述，若有不清楚的同学可以再看一遍 [SparkRpcCore-MasterImplement](http://localhost:4000/2018/05/02/SparkRpcCore-MasterImplement/) 关于 setupEndpoint 部分的实现
{% endnote %}

{% note danger %}
接下来我们跳过 rpcEnv.setupEndpoint() 直接看 Endpoint.OnStart() ，也就是 Worker().OnStart()
{% endnote %}

## Worker 向 Master 注册

### OnStart() && registerWithMaster() && tryRegisterAllMasters()

{% codeblock lang:scala OnStart https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala Worker.scala %}
override def onStart() {
  [...]
  // 向 Master 注册 Worker
  registerWithMaster()
  [...]
}
{% endcodeblock %}

{% codeblock lang:scala registerWithMaster https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala Worker.scala %}
private def registerWithMaster() {
  [...]
  registerMasterFutures = tryRegisterAllMasters()
  [...]
}
{% endcodeblock %}

{% codeblock lang:scala tryRegisterAllMasters https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala Worker.scala %}
private def tryRegisterAllMasters(): Array[JFuture[_]] = {
  // 根据 Master 地址的数量，启动多个线程并行将 Worker 注册给每一个 Master
  masterRpcAddresses.map { masterAddress =>
    registerMasterThreadPool.submit(new Runnable {
      override def run(): Unit = {
        try {
          logInfo("Connecting to master " + masterAddress + "...")
          // 根据 Master 的地址和 ENDPOINT_NAME 获取 Master 的 NettyRpcEndpointRef
          // 注意: rpcEnv 是 Worker 的 NettyRpcEnv
          val masterEndpoint = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
          // 发送注册 Worker 的消息给 Master
          sendRegisterMessageToMaster(masterEndpoint)
        } catch {
          case ie: InterruptedException => // Cancelled
          case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
        }
      }
    })
  }
}
{% endcodeblock %}

#### NettyRpcEnv().setupEndpointRef() && RpcEnv().setupEndpointRef()

{% note info %}
之前已经了解过 rpcEnv.setupEndpoint()
我们现在看看 rpcEnv.setupEndpointRef() 都干了什么事情
{% endnote %}

{% note danger %}
由于 NettyRpcEnv 没有 setupEndpointRef 方法，所以去它的父类 RpcEnv 中查找
{% endnote %}

{% codeblock lang:scala RpcEnv https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/RpcEnv.scala RpcEnv.scala %}
// 会调用 setupEndpointRefByURI
def setupEndpointRef(address: RpcAddress, endpointName: String): RpcEndpointRef = {
  setupEndpointRefByURI(RpcEndpointAddress(address, endpointName).toString)
}
// 调用 asyncSetupEndpointRefByURI ，并同步等待结果
def setupEndpointRefByURI(uri: String): RpcEndpointRef = {
  defaultLookupTimeout.awaitResult(asyncSetupEndpointRefByURI(uri))
}
// 这里由于 asyncSetupEndpointRefByURI 是抽象方法，所以调用子类方法
def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]
{% endcodeblock %}

{% codeblock lang:scala NettyRpcEnv https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala NettyRpcEnv.scala %}
def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
  // uri -> MasterURI
  val addr = RpcEndpointAddress(uri)

  // this -> Worker 的 NettyRpcEnv
  val endpointRef = new NettyRpcEndpointRef(conf, addr, this)

  // 初始化了一个 RpcEndpointVerifier 的 NettyRpcEndpointRef
  // addr.rpcAddress -> Master.rpcAddress （接受方）
  // RpcEndpointVerifier.NAME -> "endpoint-verifier" （接受方）
  // this -> Worker 的 NettyRpcEnv （发送方）
  val verifier = new NettyRpcEndpointRef(conf, RpcEndpointAddress(addr.rpcAddress, RpcEndpointVerifier.NAME), this)

  // endpointRef.name -> Master 的 ENDPOINT_NAME
  verifier.ask[Boolean](RpcEndpointVerifier.CheckExistence(endpointRef.name)).flatMap { find =>
    if (find) {
      Future.successful(endpointRef)
    } else {
      Future.failed(new RpcEndpointNotFoundException(uri))
    }
  }(ThreadUtils.sameThread)
}
{% endcodeblock %}

{% note danger %}
**特别注意：**
NettyRpcEndpointRef 类初始化时需要三个参数：1. SparkConf 2. RpcEndpointAddress 3. NettyRpcEnv
NettyRpcEndpointRef 类的作用就是发消息，将 NettyRpcEnv.address 作为 **发送方** ，将 RpcEndpointAddress.address 作为 **接收方**
**也就是说消息会从 NettyRpcEnv.address 发送给 RpcEndpointAddress.address ！**
**这点是判断 RPC 到底使用 本地模式 还是 远程模式 的根本判断条件！**
{% endnote %}

#### NettyRpcEndpointRef().ask() && RequestMessage

{% codeblock lang:scala NettyRpcEndpointRef https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala NettyRpcEnv.scala %}
// 这里会将消息包装成为 RequestMessage
override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
  nettyEnv.ask(new RequestMessage(nettyEnv.address, this, message), timeout)
}
{% endcodeblock %}

{% codeblock lang:scala RequestMessage https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala NettyRpcEnv.scala %}
private[netty] class RequestMessage(
    val senderAddress: RpcAddress, // 发送方
    val receiver: NettyRpcEndpointRef, // 接收方
    val content: Any) {
    [...]
}
{% endcodeblock %}

{% note danger %}
这里最重要的点是 RequestMessage 第一个参数是发送者，第二个参数是接受者，正好和 NettyRpcEndpointRef 的顺序相反
{% endnote %}

#### NettyRpcEnv().ask()

{% note danger %}
该方法很重要，需要完全掌握
{% endnote %}

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala NettyRpcEnv.scala %}
private[netty] def ask[T: ClassTag](message: RequestMessage, timeout: RpcTimeout): Future[T] = {
  val promise = Promise[Any]()
  // RequestMessage.NettyRpcEndpointRef.address
  val remoteAddr = message.receiver.address

  def onFailure(e: Throwable): Unit = {
    if (!promise.tryFailure(e)) {
      e match {
        case e : RpcEnvStoppedException => logDebug (s"Ignored failure: $e")
        case _ => logWarning(s"Ignored failure: $e")
      }
    }
  }
  def onSuccess(reply: Any): Unit = reply match {
    case RpcFailure(e) => onFailure(e)
    case rpcReply =>
      if (!promise.trySuccess(rpcReply)) {
        logWarning(s"Ignored message: $reply")
      }
  }
  try {
    // remoteAddr -> 接收方地址 RequestMessage.NettyRpcEndpointRef.address ，当前是 Master.address
    // address -> 发送方地址 NettyRpcEnv.address ，当前是 Worker.address
    if (remoteAddr == address) {
      val p = Promise[Any]()
      p.future.onComplete {
        case Success(response) => onSuccess(response)
        case Failure(e) => onFailure(e)
      }(ThreadUtils.sameThread)
      dispatcher.postLocalMessage(message, p)
    } else { // 所以 verifier 的 ask 是远程 RPC 调用
      // this -> NettyRpcEnv (WorkerNettyRpcEnv)
      // 这里序列化消息时会调用 nettyEnv.serializeStream(out) -> javaSerializerInstance.serializeStream(out)
      val rpcMessage = RpcOutboxMessage(message.serialize(this), onFailure, (client, response) => onSuccess(deserialize[Any](client, response)))
      // message.receiver -> NettyRpcEndpointRef (verifier)
      postToOutbox(message.receiver, rpcMessage)
      promise.future.failed.foreach {
        case _: TimeoutException => rpcMessage.onTimeout()
        case _ =>
      }(ThreadUtils.sameThread)
    }

    val timeoutCancelable = timeoutScheduler.schedule(new Runnable {
      override def run(): Unit = {
        onFailure(new TimeoutException(s"Cannot receive any reply from ${remoteAddr} " +
          s"in ${timeout.duration}"))
      }
    }, timeout.duration.toNanos, TimeUnit.NANOSECONDS)
    promise.future.onComplete { v =>
      timeoutCancelable.cancel(true)
    }(ThreadUtils.sameThread)
  } catch {
    case NonFatal(e) =>
      onFailure(e)
  }
  promise.future.mapTo[T].recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread)
}
{% endcodeblock %}

#### NettyRpcEnv().postToOutbox()

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala NettyRpcEnv.scala %}
private def postToOutbox(receiver: NettyRpcEndpointRef, message: OutboxMessage): Unit = {
  if (receiver.client != null) {
    message.sendWith(receiver.client)
  } else {
    val targetOutbox = {
      // outboxes -> new ConcurrentHashMap[RpcAddress, Outbox]()
      // receiver.address -> Master.address
      val outbox = outboxes.get(receiver.address)
      if (outbox == null) {
        // this -> NettyRpcEnv (WorkerNettyRpcEnv)
        // receiver.address -> Master 的 address
        val newOutbox = new Outbox(this, receiver.address)
        // 这里将 newOutbox 添加到了 outboxes 中，地址是 Master 的 address
        val oldOutbox = outboxes.putIfAbsent(receiver.address, newOutbox)
        if (oldOutbox == null) {
          newOutbox
        } else {
          oldOutbox
        }
      } else {
        outbox
      }
    }
    if (stopped.get) {
      outboxes.remove(receiver.address)
      targetOutbox.stop()
    } else {
      // targetOutbox -> newOutbox
      targetOutbox.send(message)
    }
  }
}
{% endcodeblock %}

#### OutBox().send()

{% codeblock lang:scala - https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/Outbox.scala Outbox.scala %}
def send(message: OutboxMessage): Unit = {
  val dropped = synchronized {
    if (stopped) {
      true
    } else {
      // messages -> new java.util.LinkedList[OutboxMessage]
      messages.add(message)
      false
    }
  }
  if (dropped) {
    message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
  } else {
    drainOutbox()
  }
}
{% endcodeblock %}

#### OutBox().drainOutbox() && launchConnectTask()

{% codeblock lang:scala drainOutbox https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/Outbox.scala Outbox.scala %}
private def drainOutbox(): Unit = {
  [...]
  if (client == null) {
    // 第一次调用时，是没有 client 的，需要先初始化 client
    launchConnectTask()
    return
  }
  [...]
}
{% endcodeblock %}

{% codeblock lang:scala launchConnectTask https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/rpc/netty/Outbox.scala Outbox.scala %}
private def launchConnectTask(): Unit = {
  // 向线程池中提交一个线程处理创建 TransportClient
  connectFuture = nettyEnv.clientConnectionExecutor.submit(new Callable[Unit] {
    override def call(): Unit = {
      try {
        // 调用
        val _client = nettyEnv.createClient(address)
        outbox.synchronized {
          client = _client
          if (stopped) {
            closeClient()
          }
        }
      } catch {
        [...]
      }
      outbox.synchronized { connectFuture = null }
      // 完成后会再次调用 drainOutbox 方法
      drainOutbox()
    }
  })
}
{% endcodeblock %}


`-EOF-`
