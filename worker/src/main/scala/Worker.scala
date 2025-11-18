package worker

import io.grpc.ManagedChannelBuilder
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

import proto.common._                  // WorkerData, WorkerDataResponse 등
import proto.common.MasterServiceGrpc  // gRPC 서비스 바인딩

object Worker extends App {
  val host = "localhost"
  val port = 9000

  val channel =
    ManagedChannelBuilder
      .forAddress(host, port)
      .usePlaintext() // TLS 안 씀
      .build()

  val stub: MasterServiceGrpc.MasterServiceStub =
    MasterServiceGrpc.stub(channel)

  println(s"[WORKER] Connecting to master at $host:$port ...")

  val request = WorkerData(
    fileSize = 123456L,
    workerHost = host,
    workerPort = port
  )

  println("[WORKER] Sending SendWorkerData RPC...")
  val responseF: Future[WorkerDataResponse] =
    stub.registerWorker(request)

  Await.result(responseF, 5.seconds)

  println("[WORKER] RPC finished successfully.")

  channel.shutdownNow()
}
