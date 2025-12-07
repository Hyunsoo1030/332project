package worker

import io.grpc.{ManagedChannel, Server}
import proto.common.WorkerServiceGrpc.WorkerServiceStub

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Promise}

/** 다른 워커와의 채널 정보 */
final case class RemoteWorkerEntry(
                                    host: String,
                                    port: Int,
                                    order: Int,
                                    channel: ManagedChannel,
                                    stub: WorkerServiceStub
                                  )

/**
 * Worker 전체에서 공유하는 상태를 모아두는 객체.
 * (나중에 WorkerServiceImpl / Worker에서 WorkerState를 사용하도록 교체하면 됨)
 */
object WorkerState {
  implicit val ec: ExecutionContext = ExecutionContext.global

  // 이 워커 자신의 주소
  var workerHost: String = "127.0.0.1"
  var workerPort: Int    = -1
  var myOrder: Int       = -1

  // 다른 워커들과의 gRPC 채널
  val remoteWorkers: ListBuffer[RemoteWorkerEntry] = ListBuffer.empty

  // pivot 목록
  var pivotsList: List[String] = Nil

  // 샘플링 이후 단계에서 사용하는 완료 플래그들
  val sortComplete: Promise[Unit]    = Promise[Unit]()
  val shuffleComplete: Promise[Unit] = Promise[Unit]()

  // 필요하다면 서버 참조도 여기로 빼둘 수 있음 (shutdown용)
  var server: Option[Server] = None

  /** 테스트나 재시작 시 상태를 초기화할 때 쓰면 편함 */
  def reset(): Unit = {
    workerHost = "127.0.0.1"
    workerPort = -1
    myOrder    = -1
    pivotsList = Nil
    remoteWorkers.clear()
    // Promise는 재할당이 필요하지만, 보통 테스트에서만 reset()을 쓰면
    // 새 WorkerState 객체를 만드는 편이 더 깔끔함. 여기서는 간단히 두고 감.
  }
}
