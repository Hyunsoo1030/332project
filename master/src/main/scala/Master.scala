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
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
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

  // 모든 worker가 readyToShuffle을 호출했는지 확인
  val ShuffleReadyFlags: mutable.Map[(String, Int), Boolean] = mutable.Map.empty
  val whenAllReadyToShuffle = Promise[Unit]()

  // 모든 worker가 "readyToShuffle"를 보낸 뒤에는 Shuffle phase 시작
  Master.whenAllReadyToShuffle.future.onComplete {
    case Success(_) =>
      println("=====================================================")
      println("[MASTER] All workers are ready. Starting shuffle phase.")
      println("=====================================================")

      // 모든 워커 주소를 Address 리스트로 구성 (order = 인덱스)
      val addresses: List[Address] =
        Master.workers.zipWithIndex.map { case (w, idx) =>
          Address(
            ip   = w.host,
            port = w.port,
            order = idx
          )
        }.toList

      // 각 워커에게 자기 partition 번호(myOrder)를 알려주면서 startShuffle 호출
      Master.workers.zipWithIndex.foreach { case (w, idx) =>
        val req = ShuffleRequest()

        println(s"[MASTER] Sending startShuffle to worker#$idx ${w.host}:${w.port} ...")

        w.stub.startShuffle(req).onComplete {
          case Success(_) =>
            println(s"[MASTER] Shuffle finished on worker#$idx (${w.host}:${w.port})")
          case Failure(e) =>
            println(s"[MASTER] Shuffle failed on worker#$idx: ${e.getMessage}")
        }
      }

    case Failure(e) =>
      println(s"[MASTER] whenAllReadyToShuffle failed: ${e.getMessage}")
  }

  // 모든 worker가 readyToMerge를 호출했는지 확인
  val MergeReadyFlags: mutable.Map[(String, Int), Boolean] = mutable.Map.empty
  val whenAllReadyToMerge = Promise[Unit]()

  // 모든 worker가 shuffling을 완료하면 merge 시작
  Master.whenAllReadyToMerge.future.onComplete {
    case Success(_) =>
      println("=====================================================")
      println("[MASTER] All workers are ready. Starting merge phase.")
      println("=====================================================")

      // 각 worker에 대한 startMerge 호출 Future를 모은다
      val mergeFutures: List[Future[MergeResponse]] =
        Master.workers.zipWithIndex.map { case (w, idx) =>
          val req = MergeRequest()

          println(s"[MASTER] Sending startMerge to worker#$idx ${w.host}:${w.port} ...")

          val f = w.stub.startMerge(req)
          // 개별 워커 로그는 그대로 유지
          f.onComplete {
            case Success(_) =>
              println(s"[MASTER] Merge finished on worker#$idx (${w.host}:${w.port})")
            case Failure(e) =>
              println(s"[MASTER] Merge failed on worker#$idx: ${e.getMessage}")
          }
          f
        }.toList

      Future.sequence(mergeFutures).onComplete {
        case Success(_) =>
          println("=====================================================")
          println("[MASTER] All merges finished. Final outputs are ready.")
          println("=====================================================")
          println("Press ENTER to terminate master server.")
        case Failure(e) =>
          println(s"[MASTER] Merge phase failed: ${e.getMessage}")
      }

    case Failure(e) =>
      println(s"[MASTER] whenAllReadyToMerge failed: ${e.getMessage}")
  }


  // 플래그: 정상 종료 중인지 여부
  @volatile var normalShutdownInProgress = false

  // 비상용 종료 플로우 - 서버만 종료
  sys.addShutdownHook {
    if (!normalShutdownInProgress) {
      println("[MASTER] [EMERGENCY] Shutting down gRPC server only...")
      server.shutdown()
    } else {
      // 정상 종료 중이면 훅에서는 아무것도 안 함
      println("[MASTER] Normal shutdown in progress, skip shutdownHook server.shutdown()")
    }
  }

  // 정상 종료 플로우
  println("Press ENTER to terminate master server.")
  StdIn.readLine()

  println("[MASTER] Sending shutdown RPC to all workers...")

  normalShutdownInProgress = true  // 정상 종료 시작 표시

  // 모든 워커에게 병렬로 Shutdown 보내기
  val shutdownFutures: List[Future[Unit]] =
    workers.map { w =>
      w.stub.shutdown(ShutdownRequest()).map { _ =>
        println(s"[MASTER] Worker shutdown ACK received from ${w.host}:${w.port}")
        ()
      }
    }.toList

  // 전체 Future 완료를 기다리기 위한 Promise
  val whenAllShutdownDone = Promise[Unit]()

  Future.sequence(shutdownFutures).onComplete { _ =>
    println("[MASTER] All shutdown RPCs completed. Stopping master gRPC server...")
    server.shutdown()
    println("[MASTER] Master terminated.")
    whenAllShutdownDone.trySuccess(())
  }

  // 여기서 main 스레드가 끝까지 기다리게 함
  Await.result(whenAllShutdownDone.future, Duration.Inf)

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

  private var nextPort: Int = 50500

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

    Master.ShuffleReadyFlags += ((req.workerHost, req.workerPort) -> false)

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

  override def readyToShuffle(req: ReadyRequest): Future[ReadyResponse] = Future {
    val key = (req.host, req.port)

    // 해당 worker가 Master.workers 목록에 존재하는지 확인
    if (!Master.ShuffleReadyFlags.contains(key)) {
      println(s"[MASTER] WARNING: readyToShuffle received from unknown worker ${req.host}:${req.port}")
    } else {
      // ready 표시
      Master.ShuffleReadyFlags.update(key, true)
      println(s"[MASTER] Worker at ${req.host}:${req.port} is ready to shuffle.")
    }

    // 모든 worker가 ready 되었는지 검사
    val allReady = Master.ShuffleReadyFlags.values.forall(_ == true)

    if (allReady && !Master.whenAllReadyToShuffle.isCompleted) {
      println("=====================================================")
      println("[MASTER] All workers are ready to shuffle!")
      println("=====================================================")
      Master.whenAllReadyToShuffle.trySuccess(())
    }

    ReadyResponse()
  }

  override def readyToMerge(req: ReadyRequest) : Future[ReadyResponse] = Future {
    val key = (req.host, req.port)

    if (!Master.MergeReadyFlags.contains(key)) {
      Master.MergeReadyFlags += (key -> false)
    }

    Master.MergeReadyFlags.update(key, true)
    println(s"[MASTER] Worker at ${req.host}:${req.port} is ready to merge.")

    val allReady = Master.MergeReadyFlags.values.forall(_ == true)

    if (allReady && !Master.whenAllReadyToMerge.isCompleted) {
      println("===============================================")
      println("[MASTER] All workers finished shuffle! Starting merge phase.")
      println("===============================================")
      Master.whenAllReadyToMerge.trySuccess(())
    }

    ReadyResponse()
  }

}