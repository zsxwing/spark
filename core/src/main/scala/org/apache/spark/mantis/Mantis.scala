package org.apache.spark.mantis

import java.io.Serializable
import java.net.Socket
import java.nio.ByteBuffer
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.rpc._
import org.apache.spark.util.RpcUtils
import org.apache.spark._
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.serializer.JavaSerializer
import rx.lang.scala.subjects.{ReplaySubject, PublishSubject}
import scala.collection.mutable

import scala.io.Source
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import rx.lang.scala.{Subscription, Observable}
import rx.lang.scala._
import rx.lang.scala.JavaConversions._

import _root_.io.reactivex.netty.RxNetty
import _root_.io.reactivex.netty.channel.ConnectionHandler
import _root_.io.reactivex.netty.channel.ObservableConnection
import _root_.io.reactivex.netty.pipeline.PipelineConfigurators
import _root_.io.reactivex.netty.server.RxServer
import scala.language.implicitConversions

import scala.concurrent.duration._

trait MantisJob[T] extends Serializable {

  val id: Int = MantisJob.nextId

  protected val engine: ExecutionEngine

  def stage[R: ClassTag](f: Observable[T] => Observable[R], partitioner: Partitioner): MantisStage[T, R] = {
    new MantisStage(engine, this, partitioner, f)
  }

  def stage[R: ClassTag](f: Observable[T] => Observable[R], numPartitions: Int): MantisStage[T, R] = {
    stage(f, new HashPartitioner(numPartitions))
  }

  def stage[R: ClassTag](f: Observable[T] => Observable[R]): MantisStage[T, R] = {
    stage(f, 2)
  }

  def sink(sink: () => Observer[T]): Unit
}

class MantisSource[T](val engine: ExecutionEngine, source: () => Observable[T]) extends MantisJob[T] {

  override def sink(sink: () => Observer[T]): Unit = source().subscribe(sink())
}

class MantisStage[T, R: ClassTag](
  val engine: ExecutionEngine,
  parent: MantisJob[T],
  partitioner: Partitioner,
  f: Observable[T] => Observable[R]) extends MantisJob[R] {

  override def sink(sink: () => Observer[R]): Unit = {
    parent.sink(engine.exchange(id, partitioner, f, sink))
  }

}

object MantisJob {

  private val idGenerator = new AtomicInteger(0)

  def nextId: Int = idGenerator.getAndIncrement()

  //val engine = new LocalExecutionEngine
  private val engine = new ClusterExecutionEngine

  def source[T](source: () => Observable[T]): MantisSource[T] = {
    new MantisSource(engine, source)
  }
}

object Sinks {

  def print[T]: () => Observer[T] = { () =>
    new Observer[T] {
      override def onNext(value: T): Unit = println(value)

      override def onError(error: Throwable): Unit = error.printStackTrace()

      override def onCompleted(): Unit = {}
    }
  }
}

object Sources {

  def socketTextSource(host: String, port: Int): () => Observable[String] = { () =>
    Observable.using(new Socket(host, port))(socket => {
      Observable[String](subscriber => {
        try {
          println("Connecting to " + host + ":" + port)
          val iter = Source.fromInputStream(socket.getInputStream).getLines()
          println("Connected to " + host + ":" + port)
          while (!subscriber.isUnsubscribed && iter.hasNext) {
            val v = iter.next()
            subscriber.onNext(v)
          }
          if (!subscriber.isUnsubscribed) {
            subscriber.onCompleted()
          }
        } catch {
          case NonFatal(e) =>
            if (!subscriber.isUnsubscribed) {
              subscriber.onError(e)
            }
        }
      })
    }, _.close()).retryWhen { attempts =>
      attempts.zip(1 to Int.MaxValue) flatMap { case (_, i) =>
        val delaySeconds = math.pow(2, i).toInt
        println(s"Will retry to connect to $host:$port in $delaySeconds second(s)")
        Observable.timer(delaySeconds.seconds)
      }
    }
  }
}

class ExchangeSink[T: ClassTag](host: String, port: Int, subscription: Subscription) extends (Observable[T] => Unit) {

  private val serializer = new JavaSerializer(new SparkConf()).newInstance()

  private val connectionObservable =
    RxNetty.createTcpClient("localhost", port, PipelineConfigurators.byteArrayConfigurator()).connect()

  override def apply(o: Observable[T]): Unit = {
    toScalaObservable(connectionObservable).subscribe(
      connection => {
        o.doOnUnsubscribe {
          subscription.unsubscribe()
        }.subscribe(v => {
          connection.writeAndFlush(JavaUtils.bufferToArray(serializer.serialize(v))): Unit
        }, e => e.printStackTrace())
      },
      e => e.printStackTrace(),
      () => ()
    )
  }
}

class ExchangeSource[T: ClassTag](val partitionId: Int) extends (() => Observable[T]) {

  private val subject = ReplaySubject[T]().toSerialized

  private val serializer = new JavaSerializer(new SparkConf()).newInstance()

  private def createServer(): RxServer[Array[Byte], Array[Byte]] = {
    RxNetty.createTcpServer[Array[Byte], Array[Byte]](0, PipelineConfigurators.byteArrayConfigurator(),
      new ConnectionHandler[Array[Byte], Array[Byte]]() {
        override def handle(newConnection: ObservableConnection[Array[Byte], Array[Byte]]): rx.Observable[Void] = {
          toJavaObservable(toScalaObservable(newConnection.getInput).map { (bytes: Array[Byte]) =>
            serializer.deserialize[T](ByteBuffer.wrap(bytes))
          }.doOnEach(
              v => {
                subject.onNext(v)
              },
              e => subject.onError(e),
              () => subject.onCompleted()
            ).map(_ => null: Void)).asInstanceOf[rx.Observable[Void]]
        }
      })
  }

  override def apply(): Observable[T] = subject

  private val server = createServer().start()

  def await(): Unit = {
    server.waitTillShutdown()
  }

  val port = server.getServerPort
}


trait ExecutionEngine extends Serializable {

  def defaultParallelism: Int

  def exchange[T, R: ClassTag](
    id: Int,
    partitioner: Partitioner,
    f: Observable[T] => Observable[R],
    sink: () => Observer[R]): () => Observer[T]
}

case class RegisterSink(id: Int)

case class LaunchExchangeSource[T](sinkId: Int, cores: Int, sink: () => Observer[T])

case class ShutdownExchangeSource[T](sinkId: Int)

case class SinkAddress(sinkId: Int, partitionId: Int, host: String, port: Int)

case class SinkCallback(sinkId: Int, callback: ((String, Int) => Unit))

class ClusterExecutionEngine extends ExecutionEngine {

  import ClusterExecutionEngine._

  val defaultParallelism: Int = 4

  @transient private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Mantis")
  @transient private val sparkContext = new SparkContext(sparkConf)

  sparkContext.setLogLevel("ERROR")

  sparkContext.env.rpcEnv.setupEndpoint(COORDINATOR_ENDPOINT_NAME, new ThreadSafeRpcEndpoint {

    override val rpcEnv: RpcEnv = sparkContext.env.rpcEnv

    private val sinkIdToObservable = new mutable.HashMap[Int, (Int, mutable.ArrayBuffer[RpcCallContext])]()
    private val sinkIdToExchangeSource =  new mutable.HashMap[Int, mutable.HashMap[Int, (String, Int)]]()

    override def receive: PartialFunction[Any, Unit] = {
      case d@SinkAddress(id, partitionId, host, port) =>
        sinkIdToExchangeSource(id)(partitionId) = (host, port)
        if (sinkIdToExchangeSource(id).size == sinkIdToObservable(id)._1) {
          val m = sinkIdToExchangeSource(id).toMap
          for (c <- sinkIdToObservable(id)._2) {
            c.reply(m)
          }
          sinkIdToObservable(id)._2.clear()
        }
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case LaunchExchangeSource(id, numPartitions, sink) =>
        if (sinkIdToObservable.contains(id)) {
          if (sinkIdToExchangeSource(id).size == sinkIdToObservable(id)._1) {
            val m = sinkIdToExchangeSource(id).toMap
            for (c <- sinkIdToObservable(id)._2) {
              c.reply(m)
            }
            sinkIdToObservable(id)._2.clear()
            context.reply(m)
          } else {
            sinkIdToObservable(id)._2 += context
          }
        } else {
          sinkIdToObservable(id) = (numPartitions, mutable.ArrayBuffer(context))
          sinkIdToExchangeSource(id) = mutable.HashMap.empty
          var i = 0
          while (i < numPartitions) {
            val rdd = sparkContext.makeRDD(Seq(i), 1)
            rdd.foreachAsync { partitionId =>
              val coordinatorRef = RpcUtils.makeDriverRef(COORDINATOR_ENDPOINT_NAME, SparkEnv.get.conf, SparkEnv.get.rpcEnv)
              val exchangeSource = new ExchangeSource(partitionId)
              exchangeSource().subscribe(sink())
              coordinatorRef.send(
                SinkAddress(id, partitionId, SparkEnv.get.conf.get("spark.driver.host", "localhost"), exchangeSource.port)
              )
              exchangeSource.await()
            }
            i += 1
          }
        }
      case ShutdownExchangeSource(sinkId) =>
      // TODO
    }
  })

  override def exchange[T, R: ClassTag](id: Int,
    partitioner: Partitioner,
    f: Observable[T] => Observable[R],
    sink: () => Observer[R]): () => Observer[T] = { () =>
    val p = ReplaySubject[T].toSerialized

    val coordinatorRef = RpcUtils.makeDriverRef(COORDINATOR_ENDPOINT_NAME, SparkEnv.get.conf, SparkEnv.get.rpcEnv)
    val hostPosts = coordinatorRef.askWithRetry[Map[Int, (String, Int)]](LaunchExchangeSource(id, partitioner.numPartitions, sink))

    f(p).groupBy(partitioner.getPartition).subscribe(
    { case (partitionId, group) =>
      val (host, port) = hostPosts(partitionId)
      val exchangeSink = new ExchangeSink[R](host, port, Subscription {
        //coordinatorRef.send(ShutdownExchangeSource(sinkId))
      })
      exchangeSink(group)
    },
    e => e.printStackTrace(),
    () => Unit // TODO
    )
    p
  }
}

object ClusterExecutionEngine {
  private val COORDINATOR_ENDPOINT_NAME = "Mantis-Coordinator"
}

object Demo {

  implicit def toPairObservable[K, T](o: Observable[(K, Observable[T])]) = o.map { case (key, values) =>
    values.map(value => (key, value))
  }

  def apply(): Unit = {
    MantisJob.
      source(Sources.socketTextSource("localhost", 9999)).
      stage((lines: Observable[String]) => {
          lines.flatMap { line => Observable.from(line.split(" ")) }
        }, 3).
      stage((lines: Observable[String]) => {
          lines.groupBy(word => word, _ => 1).flatMap { case (word, counts) =>
            counts.tumbling(5.seconds).flatMap { window =>
              window.sum.timestamp.map { case (time, countInWindow) =>
                (word, new Date(time), countInWindow)
              }
            }
          }
        }, 2).
      sink(Sinks.print)

    Thread.sleep(1000)
  }
}
