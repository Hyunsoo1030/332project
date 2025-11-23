package worker

import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.{ExecutionContext, Future}

import proto.common._                  // WorkerData, WorkerDataResponse 등
import proto.common.MasterServiceGrpc  // gRPC 서비스 바인딩
import proto.common.WorkerServiceGrpc

import java.nio.file.{Files, Paths, Path}
import java.io.RandomAccessFile
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._
import scala.util.Random

import com.google.protobuf.ByteString


object Worker extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global

  /*로컬 파워셸 테스트용
  $env:MASTER_IP="localhost"
  $env:MASTER_PORT="8915"
  $env:WORKER_IP="localhost"
  $env:WORKER_PORT="7777"
  $env:DATA_PATH="C:\Users\matth\Desktop\cs332Project\332project\input"
  sbt worker/run
  */

  val masterIp    = sys.env("MASTER_IP")
  val masterPort  = sys.env("MASTER_PORT").toInt
  val workerIp    = sys.env("WORKER_IP")
  val workerPort  = sys.env("WORKER_PORT").toInt
  val dataPath    = sys.env("DATA_PATH")

  val server: Server =  //본인 서버 구축
    ServerBuilder
      .forPort(workerPort)
      .addService(WorkerServiceGrpc.bindService(new WorkerServiceImpl(dataPath), ec))
      .build()
      .start()

  val channelToMaster =
    ManagedChannelBuilder
      .forAddress(masterIp, masterPort)
      .usePlaintext() // TLS 안 씀
      .build()

  val masterStub: MasterServiceGrpc.MasterServiceStub =
    MasterServiceGrpc.stub(channelToMaster)

  println(s"[WORKER] Connecting to master at $masterIp:$masterPort ...")

  val request = WorkerData(
    workerHost = workerIp,
    workerPort = workerPort
  )

  println("[WORKER] Registering with registerWorker RPC...")
  val registerResponseF: Future[WorkerDataResponse] =
    masterStub.registerWorker(request)

  registerResponseF.onComplete(_ -> {println("[WORKER] Registering complete.")})

  println("[WORKER] Awaiting master's order...")
  server.awaitTermination()

  var pivotsList: List[String] = Nil


  def samplesFromDir(dataPath: String, sampleBytes: Long): SampleResponse = {
    val RecordSize  = 100
    val KeySize     = 10
    val dir = Paths.get(dataPath)

    // 1) 디렉토리 안의 일반 파일 목록 수집
    val files = Files.list(dir).iterator().asScala
      .filter(p => Files.isRegularFile(p))
      .toVector

    if (files.isEmpty) {
      println(s"[WORKER] DATA_PATH=$dataPath is empty.")
      return SampleResponse(samples = Seq.empty)
    }

    val sizes = files.map(Files.size)
    val totalBytes = sizes.sum

    if (totalBytes <= 0 || sampleBytes <= 0) {
      return SampleResponse(samples = Seq.empty)
    }

    // 2) 샘플링할 레코드 개수 결정
    val maxBytes   = math.min(sampleBytes, totalBytes)
    val numSamples = (maxBytes / RecordSize).toInt

    if (numSamples <= 0) {
      return SampleResponse(samples = Seq.empty)
    }

    val rnd = new Random()
    val samplesBuilder = Vector.newBuilder[String]

    for (_ <- 0 until numSamples) {
      // 3) 전체 파일들을 하나로 이어 붙였다고 생각하고 전역 오프셋 선택
      val maxOffset = totalBytes - RecordSize
      val globalOffset =
        (math.abs(rnd.nextLong()) % (maxOffset + 1))

      // 4) 전역 오프셋을 특정 파일 + 파일 내 오프셋으로 매핑
      var remaining = globalOffset
      var idx       = 0
      while (idx < files.size && remaining >= sizes(idx)) {
        remaining -= sizes(idx)
        idx += 1
      }
      val file = files(idx)

      // 레코드 경계에 맞춰 정렬 (100바이트 단위)
      val alignedOffset = remaining - (remaining % RecordSize)

      // 5) 해당 파일에서 레코드 전체 읽고, 앞 10바이트 = key로 사용
      val raf = new RandomAccessFile(file.toFile, "r")
      try {
        raf.seek(alignedOffset)
        val buf = new Array[Byte](RecordSize)
        raf.readFully(buf)

        val keyBytes = java.util.Arrays.copyOfRange(buf, 0, KeySize)
        val keyStr   = new String(keyBytes, StandardCharsets.ISO_8859_1)
        samplesBuilder += keyStr
      } finally {
        raf.close()
      }
    }

    SampleResponse(samples = samplesBuilder.result())
  }
}

class WorkerServiceImpl(dataPath: String           // worker 로컬 데이터 파일 경로
                       )(implicit ec: ExecutionContext)
  extends WorkerServiceGrpc.WorkerService {

  private val SampleBytes: Long = 1024L * 1024L // 1MB

  override def shutdown(req: ShutdownRequest): Future[ShutdownResponse] = Future {
    println("[WORKER] Shutdown RPC received.")
    // 응답 객체
    ShutdownResponse()
  }.andThen { case _ =>
    // 응답 Future가 완료된 뒤에 프로세스 종료
    new Thread(() => System.exit(0)).start()
  }

  /** 디스크에서 1MB만 읽어서 Samples로 리턴 */
  override def getSamples(req: SampleRequest): Future[SampleResponse] = Future {
    println("[WORKER] getSamples called.")
    Worker.samplesFromDir(dataPath, SampleBytes)
  }

  override def sendPivots(req: PivotRequest): Future[PivotResponse] = Future {
    println("[WORKER] sendPivots called.")
    Worker.pivotsList = req.pivots.toList
    println(s"[WORKER] Acquired pivots: ${Worker.pivotsList}")
    PivotResponse()
  }
  // 나머지 RPC들은 일단 TODO로 두거나 구현해 둔다
  override def startShuffle(req: ShuffleRequest): Future[ShuffleResponse] =
    Future.successful(ShuffleResponse()) // TODO: 실제 구현

  override def sendData(req: DataRequest): Future[DataResponse] =
    Future.successful(DataResponse())    // TODO

  override def startMerge(req: MergeRequest): Future[MergeResponse] =
    Future.successful(MergeResponse())   // TODO
}

