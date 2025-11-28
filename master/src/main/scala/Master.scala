package master

import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import proto.common._
import proto.common.MasterServiceGrpc
import proto.common.WorkerServiceGrpc
import proto.common.WorkerServiceGrpc.WorkerServiceStub

import java.net._
import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.io.StdIn
import scala.util.{Failure, Success}

object Master extends App {
  // 입력 요구 사항
  if (args.length != 1) {
    System.err.println("Usage: master <#workers>")
    sys.exit(1)
  }
  val numWorkers = args(0).toInt
  implicit val ec: ExecutionContext = ExecutionContext.global

  case class WorkerEntry
  (
    host: String,
    port: Int,
    channel: io.grpc.ManagedChannel,
    stub: WorkerServiceStub
  )

  val workers: mutable.ListBuffer[WorkerEntry]
  = mutable.ListBuffer.empty[WorkerEntry]

  val host = findMyIp()
  val port = 8915
  val server: Server =
    ServerBuilder
      .forPort(port)
      .addService(MasterServiceGrpc.bindService(new MasterServiceImpl, ec))
      .build()
      .start()

  println(s"[MASTER] gRPC server started, listening on $host:$port")

  val whenRegisterCompleted = Promise[Unit]()
  doSample()

  // 비상용 종료 플로우 - 서버만 종료
  sys.addShutdownHook {
    println("[MASTER] Shutting down gRPC server...")
    server.shutdown()
  }

  // 정상 종료 플로우: ENTER 입력 → 워커들 종료 → 마스터 종료
  println("Press ENTER to terminate master server.")
  StdIn.readLine()

  println("[MASTER] Sending shutdown RPC to all workers...")

  // 모든 워커에게 병렬로 Shutdown 보내기
  val shutdownFutures: List[Future[Unit]] =
    workers.map { w =>
      w.stub.shutdown(ShutdownRequest()).map { _ =>
        println("[MASTER] Worker shutdown ACK received")
        ()
      }
    }.toList

  Future.sequence(shutdownFutures).onComplete { _ =>
    println("[MASTER] All shutdown RPCs completed. Stopping master gRPC server...")
    Thread.sleep(500)       // 꼭 넣고 싶으면 유지
    server.shutdown()
    println("[MASTER] Master terminated.")
  }

  def doSample(): Future[Unit] = Future {
    //sampling phase
    var samplesF: ListBuffer[Future[SampleResponse]] = ListBuffer.empty[Future[SampleResponse]]
    val whenSampleAcquired = Promise[Unit]()
    whenRegisterCompleted.future.onComplete(_ => {
      var succeedNum = 0
      samplesF = for (w <- workers) yield {
        val sampleF = w.stub.getSamples(SampleRequest())
        sampleF.onComplete{
          case Success(pivots) => {
            println(s"[MASTER] getSamples from ${w.host}:${w.port} succeeded." +
              s"\n[MASTER] Pivot samples: ${pivots.samples.take(5)}")
            succeedNum += 1
            if (succeedNum == workers.length) whenSampleAcquired.trySuccess(())
          }
          case Failure(e) =>
            println(s"[MASTER] getSamples from ${w.host}:${w.port} failed: ${e.getMessage}")
        }
        sampleF
      }
    })

    whenSampleAcquired.future.onComplete(_ -> { //Sample data 전부 받아왔을 때 출력 -> 다음으로 sort 시작
      println("=====================================================\n" +
              "[MASTER] All samples acquired. Start sorting samples." +
            "\n=====================================================")
    })

    // 마스터에서 직접 정렬하는 단계
    val allPivotsF: Future[Seq[SampleResponse]] =
      whenSampleAcquired.future.flatMap { _ =>
        // 여기서 samplesF: ListBuffer[Future[Pivots]] 를 한 번에 모아서
        Future.sequence(samplesF.toSeq)    // Future[Seq[Pivots]]
      }

    val whenPivotsGenerated = Promise[IndexedSeq[String]]()
    allPivotsF.onComplete {
      case Success(pivotsSeq) =>
        // pivotsSeq: Seq[Pivots]
        // 각 worker의 pivots를 전부 모아서 flatten
        val allKeys: Seq[String] = pivotsSeq.flatMap(_.samples)

        // 이제 Sampling 단계: master에서 정렬
        val sortedKeys: Seq[String] = allKeys.sorted

        println(s"[MASTER] total sampled keys = ${allKeys.size}")
        println(s"[MASTER] first few sorted sample keys = ${sortedKeys.take(10)}")

        // 여기서 global pivot 뽑기 (예: 워커 수 - 1 개 선택)
        val numWorkers = workers.size
        if (numWorkers > 1 && sortedKeys.nonEmpty) {
          val step = sortedKeys.size.toDouble / (numWorkers)
          val globalPivots =
            (1 until numWorkers).map(i => sortedKeys((i * step).toInt))

          println(s"[MASTER] global pivots = $globalPivots")
          whenPivotsGenerated.trySuccess(globalPivots)

        }

      case Failure(e) =>
        println(s"[MASTER] allPivotsF failed: ${e.getMessage}")
    }

    // 워커로 Pivots 전송하는 단계
    whenPivotsGenerated.future.onComplete{pv =>
      val sendPivotFutures: List[Future[PivotResponse]] =
        workers.map { w =>
          w.stub.sendPivots(PivotRequest(
            pivots=pv.get,myOrder=workers.indexOf(w), orderedWorkerData=workers.map(w => OrderedWorkerData(
              order=workers.indexOf(w), workerHost=w.host, workerPort=w.port
            )).toList
          ))
        }.toList
      Future.sequence(sendPivotFutures).onComplete { _ =>
        println("[MASTER] All pivots are sent to workers.")
        println("=================================\n" +
                "[MASTER] Sampling phase finished." +
              "\n=================================")
        println("Press ENTER to terminate master server.")
      }
    }
  }

  def findMyIp(): String = {
    val nets = NetworkInterface.getNetworkInterfaces.asScala.toList

    val addrs =
      for {
        net <- nets
        if net.isUp && !net.isLoopback && !net.isVirtual
        addr <- net.getInetAddresses.asScala
        if addr.isInstanceOf[Inet4Address]
      } yield addr.getHostAddress

    addrs.headOption.getOrElse("127.0.0.1")
  }
}



// 실제 RPC 구현체
class MasterServiceImpl(implicit ec: ExecutionContext)
  extends MasterServiceGrpc.MasterService {

  private var nextPort: Int = 50509

  override def getNewPort(req: PortRequest): Future[PortResponse] = Future {
    val p = this.synchronized {
      val cur = nextPort
      nextPort += 1
      cur
    }
    PortResponse(portNum = p)
  }

  override def registerWorker(req: WorkerData): Future[WorkerDataResponse] = {
    println(
      s"[MASTER] Received WorkerData: workerHost=${req.workerHost}, workerPort=${req.workerPort}"
    )

    // 1) 워커 아이피/포트/채널/스텁 등록
    val channel = ManagedChannelBuilder
      .forAddress(req.workerHost, req.workerPort)
      .usePlaintext()
      .build()

    val stub = WorkerServiceGrpc.stub(channel)

    stub.notifyConnection(ConnectionRequest(host=Master.host, port=Master.port))

    Master.workers += Master.WorkerEntry(req.workerHost, req.workerPort, channel, stub)

    println(
      s"[MASTER] Worker${Master.workers.length-1} with address ${req.workerHost}:${req.workerPort} registered."
    )

    //등록 완료되면 sampling phase 시작하기 위함
    if (Master.workers.length == Master.numWorkers) { // test 위해 값 바꿀 수 있음, default 값은 20 유지
      println("================================================================\n" +
              "[MASTER] All workers have registered. Initialize sampling phase." +
            "\n================================================================")
      Master.whenRegisterCompleted.trySuccess(())
    }

    // registerWorker 의 응답은 그냥 바로 성공 리턴
    Future.successful(WorkerDataResponse())
  }

  override def notifyConnection(req: ConnectionRequest): Future[ConnectionResponse] = Future {
    println(s"[MASTER] Connected from ${req.host}:${req.port}.")
    ConnectionResponse()
  }

}
