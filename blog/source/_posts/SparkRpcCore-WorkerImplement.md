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

`-EOF-`
