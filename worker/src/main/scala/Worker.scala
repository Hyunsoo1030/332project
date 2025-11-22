package worker

import io.grpc.ManagedChannelBuilder
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.{ExecutionContext, Future}

import proto.common._                  // WorkerData, WorkerDataResponse 등
import proto.common.MasterServiceGrpc
import proto.common.WorkerServiceGrpc

import java.nio.file.{Files, Paths}
import java.io.RandomAccessFile

import com.google.protobuf.ByteString


object Worker extends App {

   val masterIp    = sys.env("MASTER_IP")
   val masterPort  = sys.env("MASTER_PORT").toInt
   val workerIp      = sys.env("WORKER_IP")
   val workerPort  = sys.env("WORKER_PORT").toInt
//  val masterIp = "localhost"  // 로컬 테스트용 마스터 host
//  val masterPort = 9000         // 로컬 테스트용 마스터 port
//  val workerIp = "localhost"
//  val workerPort = 7777

  val channelToMaster =
    ManagedChannelBuilder
      .forAddress(masterIp, masterPort)
      .usePlaintext() // TLS 안 씀
      .build()

  val stub: MasterServiceGrpc.MasterServiceStub =
    MasterServiceGrpc.stub(channelToMaster)

  println(s"[WORKER] Connecting to master at $masterIp:$masterPort ...")

  val request = WorkerData(
    fileSize = 123456L,
    workerHost = workerIp,
    workerPort = workerPort
  )

  println("[WORKER] Registering with registerWorker RPC...")
  val responseF: Future[WorkerDataResponse] =
    stub.registerWorker(request)

  Await.result(responseF, 5.seconds)

  println("[WORKER] RPC finished successfully.")

  channelToMaster.shutdownNow()
}

class WorkerServiceImpl(dataPath: String           // worker 로컬 데이터 파일 경로
                       )(implicit ec: ExecutionContext)
  extends WorkerServiceGrpc.WorkerService {

  private val SampleBytes: Long = 1024L * 1024L // 1MB

  /** 디스크에서 1MB만 읽어서 Pivots로 리턴 */
  override def getSamples(req: SampleRequest): Future[Pivots] = Future {
    val path = Paths.get(dataPath)
    val fileSize = Files.size(path)

    val buf = new Array[Byte](SampleBytes.toInt)
    val raf = new RandomAccessFile(path.toFile, "r")
    try {
      raf.readFully(buf)
    } finally raf.close()

    // 예시: Pivots 안에 bytes data = 1; 같은 필드가 있다고 가정
    Pivots(
    )
  }

  // 나머지 RPC들은 일단 TODO로 두거나 구현해 둔다
  override def startShuffle(req: ShuffleRequest): Future[ShuffleResponse] =
    Future.successful(ShuffleResponse()) // TODO: 실제 구현

  override def sendData(req: DataRequest): Future[DataResponse] =
    Future.successful(DataResponse())    // TODO

  override def startMerge(req: MergeRequest): Future[MergeResponse] =
    Future.successful(MergeResponse())   // TODO
}

