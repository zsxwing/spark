package org.apache.spark.mantis

import java.net.Socket
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.rpc.{ThreadSafeRpcEndpoint, RpcCallContext, RpcEnv, RpcEndpoint}
import org.apache.spark.util.RpcUtils
import org.apache.spark.{SparkEnv, SparkContext, SparkConf}
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.serializer.JavaSerializer
import rx.lang.scala.Notification.{OnCompleted, OnError, OnNext}
import rx.lang.scala.schedulers.IOScheduler
import rx.lang.scala.subjects.PublishSubject
import scala.collection.mutable

import scala.io.Source
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import rx.lang.scala.{Subscription, Observable}
import rx.lang.scala._
import rx.lang.scala.JavaConversions._

import io.reactivex.netty.RxNetty;
import io.reactivex.netty.channel.ConnectionHandler;
import io.reactivex.netty.channel.ObservableConnection;
import io.reactivex.netty.pipeline.PipelineConfigurators;
import io.reactivex.netty.server.RxServer
import scala.language.implicitConversions

import scala.concurrent.duration._

trait MantisJob[T] extends Serializable {

  val engine: ExecutionEngine

  def stage[R: ClassTag](stage: Observable[T] => Observable[Observable[R]]): MantisStage[T, R] = {
    new MantisStage(engine, this, stage)
  }

  def sink(sink: Observable[T] => Unit): Unit
}

class MantisSource[T](val engine: ExecutionEngine, val source: () => Observable[T]) extends MantisJob[T] {

  override def sink(sink: Observable[T] => Unit): Unit = sink(source())
}

class MantisStage[T, R: ClassTag](val engine: ExecutionEngine, val parent: MantisJob[T], stage: Observable[T] => Observable[Observable[R]]) extends MantisJob[R] {

  override def sink(sink: Observable[R] => Unit): Unit = {
    parent.sink(o => {
      stage(o).subscribe(
        child => engine.exchange(child, sink),
        e => e.printStackTrace
      )
    })
  }
}

object MantisJob {

  //val engine = new LocalExecutionEngine
  val engine = new ClusterExecutionEngine

  def source[T](source: () => Observable[T]): MantisSource[T] = {
    new MantisSource(engine, source)
  }
}

trait ExchangeSink[T] extends (Observable[T] => Unit) {

}

trait ExchangeSource[T] extends (() => Observable[T]) {

}

class LocalExchangeSink[T](subject: Subject[T]) extends ExchangeSink[T] {
  override def apply(o: Observable[T]): Unit = o.subscribe(subject)
}

class LocalExchangeSource[T](subject: Subject[T]) extends ExchangeSource[T] {
  override def apply(): Observable[T] = subject
}

class RemoteExchangeSink[T: ClassTag](host: String, port: Int) extends (Observable[T] => Unit) {

  private val serializer = new JavaSerializer(new SparkConf()).newInstance()

  private val connectionObservable =
    RxNetty.createTcpClient("localhost", port, PipelineConfigurators.byteArrayConfigurator()).connect()

  override def apply(o: Observable[T]): Unit = {
    toScalaObservable(connectionObservable).subscribe(
      connection => {
        println("o: " + o)
        o.subscribe(v => {
          println("ExchangeSink: " + v)
          connection.writeAndFlush(JavaUtils.bufferToArray(serializer.serialize(v))): Unit
        }, e => e.printStackTrace())
      },
      e => e.printStackTrace(),
      () => ()
    )
  }
}

class RemoteExchangeSource[T: ClassTag] extends (() => Observable[T]) {

  private val subject = PublishSubject[T]()

  private val serializer = new JavaSerializer(new SparkConf()).newInstance()

  private def createServer(): RxServer[Array[Byte], Array[Byte]] = {
    RxNetty.createTcpServer[Array[Byte], Array[Byte]](0, PipelineConfigurators.byteArrayConfigurator(),
      new ConnectionHandler[Array[Byte], Array[Byte]]() {
        override def handle(newConnection: ObservableConnection[Array[Byte], Array[Byte]]): rx.Observable[Void] = {
          toJavaObservable(toScalaObservable(newConnection.getInput).map[T] { (bytes: Array[Byte]) =>
            serializer.deserialize[T](ByteBuffer.wrap(bytes))
          }.doOnEach(
              v => subject.onNext(v),
              e => subject.onError(e),
              () => subject.onCompleted()
            ).map(_ => null: Void)).asInstanceOf[rx.Observable[Void]]
        }
      })
  }

  override def apply(): Observable[T] = subject

  private val server = createServer().start()

  val port = server.getServerPort

  def await(): Unit = {
    server.waitTillShutdown()
  }
}


trait ExecutionEngine extends Serializable {
  def exchange[T: ClassTag](o: Observable[T], sink: Observable[T] => Unit): Unit
}

class LocalExecutionEngine extends ExecutionEngine {

  def exchange[T: ClassTag](o: Observable[T], sink: Observable[T] => Unit): Unit = {
    val p = PublishSubject[T]()
    val localSink = new LocalExchangeSink[T](p)
    val localSource = new LocalExchangeSource[T](p)
    localSink(o)
    sink(localSource())
  }

}

object RegisterNewSink

case class LaunchExchangeSource[T](sinkId: Int, sink: (Observable[T] => Unit))

case class SinkAddress(sinkId: Int, host: String, port: Int)

case class SinkCallback(sinkId: Int, callback: ((String, Int) => Unit))

class ClusterExecutionEngine extends ExecutionEngine {

  import ClusterExecutionEngine._

  @transient private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Mantis")
  @transient private val sparkContext = new SparkContext(sparkConf)

  sparkContext.setLogLevel("ERROR")

  sparkContext.env.rpcEnv.setupEndpoint(COORDINATOR_ENDPOINT_NAME, new ThreadSafeRpcEndpoint {

    override val rpcEnv: RpcEnv = sparkContext.env.rpcEnv

    private val sinkIdToObservable = new mutable.HashMap[Int, RpcCallContext]()

    private var nextSinkId = 0

    override def receive: PartialFunction[Any, Unit] = {
      case d@SinkAddress(sinkId, host, port) =>
        println(d)
        sinkIdToObservable.remove(sinkId).foreach(_.reply(host, port))
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RegisterNewSink =>
        nextSinkId += 1
        context.reply(nextSinkId)
      case LaunchExchangeSource(sinkId, sink) =>
        sinkIdToObservable(sinkId) = context
        val rdd = sparkContext.makeRDD(Seq(0), 1)
        rdd.foreachAsync { _ =>
          val coordinatorRef = RpcUtils.makeDriverRef(COORDINATOR_ENDPOINT_NAME, SparkEnv.get.conf, SparkEnv.get.rpcEnv)
          val exchangeSource = new RemoteExchangeSource
          coordinatorRef.send(SinkAddress(sinkId, SparkEnv.get.conf.get("spark.driver.host", "localhost"), exchangeSource.port))
          sink(exchangeSource())
          exchangeSource.await()
        }
    }
  })

  def exchange[T: ClassTag](o: Observable[T], sink: Observable[T] => Unit): Unit = {
    val coordinatorRef = RpcUtils.makeDriverRef(COORDINATOR_ENDPOINT_NAME, SparkEnv.get.conf, SparkEnv.get.rpcEnv)
    val sinkId = coordinatorRef.askWithRetry[Int](RegisterNewSink)
    val (host, port) = coordinatorRef.askWithRetry[(String, Int)](LaunchExchangeSource(sinkId, sink))
    val exchangeSink = new RemoteExchangeSink[T](host, port)
    exchangeSink(o)
  }
}

object ClusterExecutionEngine {
  private val COORDINATOR_ENDPOINT_NAME = "Mantis-Coordinator"
}

object Demo {

  implicit def toPairObservable[K, T](o: Observable[(K, Observable[T])]) = o.map { case (key, values) =>
    values.map(value => (key, value))
  }

  def fileSink[T](o: Observable[T]): Unit = {

  }

  def socketTextSource(host: String, port: Int): () => Observable[String] = {
    () => {
      Observable.using(new Socket(host, port))(socket => {
        Observable[String](subscriber => {
          try {
            println("Connecting to " + host + ":" + port)
            val iter = Source.fromInputStream(socket.getInputStream).getLines()
            println("Connected to " + host + ":" + port)
            while (!subscriber.isUnsubscribed && iter.hasNext) {
              val v = iter.next()
              println("Emit: " + v)
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
      }, _.close())
    }
  }

  def apply(): Unit = {

    MantisJob.source(socketTextSource("localhost", 9999)).
      stage { lines =>
        lines.flatMap { line =>
          Observable.from(line.split(" "))
        }.groupBy(word => word.hashCode() % 5).map(_._2)
      }.stage { words =>
        words.groupBy(word => word).map { case (word, values) =>
          values.tumbling(10.seconds).flatMap { window =>
            window.countLong.map(count => (word, count)).timestamp
          }
        }
      }.sink(o => o.subscribe(println, e => e.printStackTrace()))

    Thread.sleep(1000)
  }
}
