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

#### RpcEnv.create()

{% note info %}
看过 SparkRpcCore-MasertImplement 的同学应该知道 RpcEnv.create() 其实最后会得到一个 NettyRpcEnv
具体实现和 SparkRpcCore-MasertImplement 中的一致
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
Worker 通过 RpcEnv.create() 方法，最终得到了 NettyRpcEnv 。
{% endnote %}

`-EOF-`
