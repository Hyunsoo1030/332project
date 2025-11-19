package master

import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import proto.common._                  // WorkerData, WorkerDataResponse 등
import proto.common.MasterServiceGrpc

import scala.concurrent.{ExecutionContext, Future}
import scala.io.StdIn  // gRPC 서비스 바인딩

// 실제 RPC 구현체
class MasterServiceImpl(implicit ec: ExecutionContext)
  extends MasterServiceGrpc.MasterService {

  override def registerWorker(req: WorkerData): Future[WorkerDataResponse] = {
    println(
      s"[MASTER] Received WorkerData: fileSize=${req.fileSize}, workerHost=${req.workerHost}, workerPort=${req.workerPort}"
    )
    Master.workerAddress = Master.workerAddress :+ (req.workerHost, req.workerPort) // 워커 주소(Host, Port) 등록
    val channel = ManagedChannelBuilder
      .forAddress(req.workerHost, req.workerPort)
      .usePlaintext()
      .build()
    Master.workerChannels = Master.workerChannels :+ channel // 워커 채널 리스트 뒤에 등록
    println(s"[MASTER] channel with worker ${Master.workerChannels.length} registered at ${req.workerHost}:${req.workerPort}.")
    Future.successful(WorkerDataResponse())
  }
}

// 서버 엔트리 포인트
object Master extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global


  var workerAddress: List[(String, Int)] = Nil // 워커 주소(Host, Port) 튜플 리스트
  var workerChannels: List[Any] = Nil

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
