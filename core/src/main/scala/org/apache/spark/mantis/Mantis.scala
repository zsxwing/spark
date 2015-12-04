package org.apache.spark.mantis

import java.io.Serializable
import java.net.Socket
import java.nio.ByteBuffer

import org.apache.spark.rpc._
import org.apache.spark.util.RpcUtils
import org.apache.spark.{SparkEnv, SparkContext, SparkConf}
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

import io.reactivex.netty.RxNetty
import io.reactivex.netty.channel.ConnectionHandler
import io.reactivex.netty.channel.ObservableConnection
import io.reactivex.netty.pipeline.PipelineConfigurators
import io.reactivex.netty.server.RxServer
import scala.language.implicitConversions

import scala.concurrent.duration._

trait MantisJob[T] extends Serializable {

  protected val engine: ExecutionEngine

  def forkJoin[I: ClassTag, R: ClassTag](
    fork: Observable[T] => Observable[Observable[I]])(
    join: Observable[Observable[I]] => Observable[Observable[R]],
    cores: Int = 1): MantisStage[T, I, R] = {
    new MantisStage(engine, cores, this)(fork)(join)
  }



  // O[T] => O[R],   O[R]
  //                 O[R]
   //                O[R]

  // O[T] => O[G] => O[G]


  // -> O[word, G] ----  O[word, G]

  // ->

  // 1234 


  // O[T] => O[O[R]] ,  O[O[R]] =>
  // 1 2 3 4 =>  1 2 x
  //             3 4 x
  //             5 6 x
  // O[R]
  // G =>

  def stage[R: ClassTag](
    fork: Observable[T] => Observable[Observable[R]],
    cores: Int = 1): MantisStage[T, R, R] = {
    new MantisStage[T, R, R](engine, cores, this)(fork)(o => o)
  }

//  def transform[R: ClassTag](stage: Observable[T] => Observable[R], cores: Int = 1): MantisStage[T, R] = {
//    new MantisStage(engine, cores, this)(o => Observable.just(stage(o)))
//  }

  def sink(sink: () => Observer[T]): Unit
}

class MantisSource[T](val engine: ExecutionEngine, source: () => Observable[T]) extends MantisJob[T] {

  override def sink(sink: () => Observer[T]): Unit = source().subscribe(sink())
}

class MantisStage[T, I: ClassTag, R: ClassTag](
  val engine: ExecutionEngine,
  cores: Int,
  parent: MantisJob[T])(
  fork: Observable[T] => Observable[Observable[I]])(
  join: Observable[Observable[I]] => Observable[Observable[R]]) extends MantisJob[R] {

  override def sink(sink: () => Observer[R]): Unit = {
    parent.sink(engine.exchange(cores, fork, join, sink))
  }

}

object MantisJob {

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
    }, _.close()).retryWhen { attempts =>
      attempts.zip(1 to Int.MaxValue) flatMap { case (_, i) =>
        val delaySeconds = math.pow(2, i).toInt
        println(s"Will retry to connect to $host:$port in $delaySeconds second(s)")
        Observable.timer(delaySeconds.seconds)
      }
    }
  }
}

class ExchangeSink[T: ClassTag](host: String, port: Int, subscription: Subscription, streamId: Int) extends (Observable[T] => Unit) {

  private val serializer = new JavaSerializer(new SparkConf()).newInstance()

  private val connectionObservable =
    RxNetty.createTcpClient("localhost", port, PipelineConfigurators.byteArrayConfigurator()).connect()

  override def apply(o: Observable[T]): Unit = {
    toScalaObservable(connectionObservable).subscribe(
      connection => {
        o.doOnUnsubscribe {
          subscription.unsubscribe()
        }.subscribe(v => {
          println("ExchangeSink: " + v)
          connection.writeAndFlush(JavaUtils.bufferToArray(serializer.serialize((streamId, v)))): Unit
        }, e => e.printStackTrace())
      },
      e => e.printStackTrace(),
      () => ()
    )
  }
}

class ExchangeSource[T: ClassTag] extends (() => Observable[Observable[T]]) {

  private val subject = ReplaySubject[Observable[T]]().toSerialized

  private val serializer = new JavaSerializer(new SparkConf()).newInstance()

  private def createServer(): RxServer[Array[Byte], Array[Byte]] = {
    RxNetty.createTcpServer[Array[Byte], Array[Byte]](0, PipelineConfigurators.byteArrayConfigurator(),
      new ConnectionHandler[Array[Byte], Array[Byte]]() {
        override def handle(newConnection: ObservableConnection[Array[Byte], Array[Byte]]): rx.Observable[Void] = {
          toJavaObservable(toScalaObservable(newConnection.getInput).map { (bytes: Array[Byte]) =>
            val v = serializer.deserialize[(Int, T)](ByteBuffer.wrap(bytes))
            println("Receive: " + v)
            v
          }.groupBy(_._1, _._2).map(_._2).doOnEach(
              v => subject.onNext(v),
              e => subject.onError(e),
              () => subject.onCompleted()
            ).map(_ => null: Void)).asInstanceOf[rx.Observable[Void]]
        }
      })
  }

  override def apply(): Observable[Observable[T]] = subject.map(_.publish(o => o))

  private val server = createServer().start()

  val port = server.getServerPort
}


trait ExecutionEngine extends Serializable {

  def defaultParallelism: Int

  def exchange[T, I: ClassTag, R: ClassTag](
    cores: Int,
    fork: Observable[T] => Observable[Observable[I]],
    join: Observable[Observable[I]] => Observable[Observable[R]],
    sink: () => Observer[R]): () => Observer[T]
}

//class LocalExecutionEngine extends ExecutionEngine {
//
//  val defaultParallelism: Int = 1
//
//  override def exchange[T, R: ClassTag](cores: Int, stage: Observable[T] => Observable[Observable[R]], sink: () => Observer[Observable[R]]): () => Observer[T] = { () =>
//    val p = PublishSubject[T]()
//    stage(p).subscribe(sink())
//    p
//  }
//
//}

object RegisterNewSink

case class LaunchExchangeSource[T, R](sinkId: Int, join: Observable[Observable[T]] => Observable[Observable[R]], sink: () => Observer[R])

case class ShutdownExchangeSource[T](sinkId: Int)

case class SinkAddress(sinkId: Int, host: String, port: Int)

case class SinkCallback(sinkId: Int, callback: ((String, Int) => Unit))

class ClusterExecutionEngine extends ExecutionEngine {

  import ClusterExecutionEngine._

  val defaultParallelism: Int = 4

  @transient private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Mantis")
  @transient private val sparkContext = new SparkContext(sparkConf)

  sparkContext.setLogLevel("ERROR")

  sparkContext.env.rpcEnv.setupEndpoint(COORDINATOR_ENDPOINT_NAME, new ThreadSafeRpcEndpoint {

    override val rpcEnv: RpcEnv = sparkContext.env.rpcEnv

    private val sinkIdToObservable = new mutable.HashMap[Int, RpcCallContext]()
    private val sinkIdToExchangeSource =  new mutable.HashMap[Int, RpcCallContext]()

    private var nextSinkId = 0

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RegisterNewSink =>
        nextSinkId += 1
        context.reply(nextSinkId)
      case LaunchExchangeSource(sinkId, join, sink) =>
        sinkIdToObservable(sinkId) = context
        val rdd = sparkContext.makeRDD(Seq(0), 1)
        rdd.foreachAsync { _ =>
          val coordinatorRef = RpcUtils.makeDriverRef(COORDINATOR_ENDPOINT_NAME, SparkEnv.get.conf, SparkEnv.get.rpcEnv)
          val exchangeSource = new ExchangeSource
          join(exchangeSource()).subscribe(
            o => o.subscribe(sink().asInstanceOf[Observer[Any]]),
            e => e.printStackTrace,
            () => Unit // TODO
          )
          coordinatorRef.askWithRetry[Boolean](
            SinkAddress(sinkId, SparkEnv.get.conf.get("spark.driver.host", "localhost"), exchangeSource.port)
          )
        }
      case d@SinkAddress(sinkId, host, port) =>
        sinkIdToObservable.remove(sinkId).foreach(_.reply(host, port))
        sinkIdToExchangeSource(sinkId) = context
      case ShutdownExchangeSource(sinkId) =>
        sinkIdToExchangeSource.remove(sinkId).foreach(_.reply(true))
    }
  })

  def exchange[T, I: ClassTag, R: ClassTag](
    cores: Int,
    fork: Observable[T] => Observable[Observable[I]],
    join: Observable[Observable[I]] => Observable[Observable[R]],
    sink: () => Observer[R]): () => Observer[T] = { () =>
    val p = ReplaySubject[T].toSerialized
    fork(p).zip((0 until cores).toObservable.repeat).groupBy(_._2, _._1).subscribe(oor => {
      val coordinatorRef = RpcUtils.makeDriverRef(COORDINATOR_ENDPOINT_NAME, SparkEnv.get.conf, SparkEnv.get.rpcEnv)
      val sinkId = coordinatorRef.askWithRetry[Int](RegisterNewSink)
      val (host, port) = coordinatorRef.askWithRetry[(String, Int)](LaunchExchangeSource(sinkId, join, sink))
      oor._2.subscribe(
        or => {
          val exchangeSink = new ExchangeSink[I](host, port, Subscription {
            println("UnsubscribE!!!!!!!!")
            //coordinatorRef.send(ShutdownExchangeSource(sinkId))
          }, oor._1)
          exchangeSink(or)
        },
        e => e.printStackTrace(),
        () => Unit // TODO
      )
    }, e => e.printStackTrace(), () => Unit)
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
    MantisJob.source(Sources.socketTextSource("localhost", 9999)).
      forkJoin(
        lines =>
          lines.flatMap { line =>
            Observable.from(line.split(" ")).map(word => (word, 1))
          }.groupBy(_._1, _._2))(
        wordCounts => {
          wordCounts.map { wordCount =>
            wordCount.map(_._2).tumbling(10.seconds).flatMap { window =>
              window.scan(0)(_ + _)
            }
          }
        }
      ).sink(Sinks.print)

    Thread.sleep(1000)
  }
}
