package master

import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import proto.common._
import proto.common.MasterServiceGrpc
import proto.common.WorkerServiceGrpc

import scala.concurrent.{ExecutionContext, Future}
import scala.io.StdIn
import scala.util.{Failure, Success}

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
//  sys.addShutdownHook {
//    println("[MASTER] Shutting down gRPC server...")
//    server.shutdown()
//  }

//  println("Press ENTER to terminate master server.")
//  StdIn.readLine()
//  server.shutdown()
}

// 실제 RPC 구현체
class MasterServiceImpl(implicit ec: ExecutionContext)
  extends MasterServiceGrpc.MasterService {

  override def registerWorker(req: WorkerData): Future[WorkerDataResponse] = {
    println(
      s"[MASTER] Received WorkerData: fileSize=${req.fileSize}, workerHost=${req.workerHost}, workerPort=${req.workerPort}"
    )

    // 1) 워커 주소/채널 등록
    Master.workerAddress = Master.workerAddress :+ (req.workerHost, req.workerPort)

    val channel = ManagedChannelBuilder
      .forAddress(req.workerHost, req.workerPort)
      .usePlaintext()
      .build()

    Master.workerChannels = Master.workerChannels :+ channel
    println(
      s"[MASTER] channel with worker ${Master.workerChannels.length} " +
        s"registered at ${req.workerHost}:${req.workerPort}."
    )

    // 2) 방금 등록된 워커에게 GetSamples 호출
    val workerStub = WorkerServiceGrpc.stub(channel)

    // 현재 SampleRequest 안에 필드가 없다고 가정
    val sampleReq = SampleRequest(0)   // or SampleRequest.defaultInstance

    val samplesF: Future[Pivots] = workerStub.getSamples(sampleReq)

    samplesF.onComplete {
      case Success(pivots) =>
        println(
          s"[MASTER] getSamples from ${req.workerHost}:${req.workerPort} succeeded."
        )
      // TODO: Pivots 안에 필드가 있으면 여기에서 println으로 내용 확인
      // 예: println(s"  pivots = $pivots")

      case Failure(e) =>
        println(
          s"[MASTER] getSamples from ${req.workerHost}:${req.workerPort} failed: ${e.getMessage}"
        )
    }

    // registerWorker 의 응답은 그냥 바로 성공 리턴
    Future.successful(WorkerDataResponse())
  }
}
