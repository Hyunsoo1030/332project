package master

import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future}
import scala.io.StdIn

import proto.common._                  // WorkerData, WorkerDataResponse 등
import proto.common.MasterServiceGrpc  // gRPC 서비스 바인딩

// 실제 RPC 구현체
class MasterServiceImpl(implicit ec: ExecutionContext)
  extends MasterServiceGrpc.MasterService {

  override def sendWorkerData(req: WorkerData): Future[WorkerDataResponse] = {
    println(
      s"[MASTER] Received WorkerData: fileSize=${req.fileSize}, workerPort=${req.workerPort}"
    )
    Future.successful(WorkerDataResponse())
  }
}

// 서버 엔트리 포인트
object Master extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global

  val port = 9000

  val server: Server =
    ServerBuilder
      .forPort(port)
      .addService(MasterServiceGrpc.bindService(new MasterServiceImpl, ec))
      .build()
      .start()

  println(s"[MASTER] gRPC server started, listening on $port")
  sys.addShutdownHook {
    println("[MASTER] Shutting down gRPC server...")
    server.shutdown()
  }

  println("Press ENTER to terminate master server.")
  StdIn.readLine()
  server.shutdown()
}
