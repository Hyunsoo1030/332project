package worker

import io.grpc.ManagedChannelBuilder
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

import proto.common._                  // WorkerData, WorkerDataResponse 등
import proto.common.MasterServiceGrpc  // gRPC 서비스 바인딩

object Worker extends App {

  val masterIp    = sys.env("MASTER_IP")
  val masterPort  = sys.env("MASTER_PORT").toInt
  val workerIp      = sys.env("WORKER_IP")
  val workerPort  = sys.env("WORKER_PORT").toInt
  //val masterIp = "localhost"  // 로컬 테스트용 마스터 host
  //val masterPort = 9000         // 로컬 테스트용 마스터 port

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
