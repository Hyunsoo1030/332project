package master

import io.grpc.ManagedChannel
import proto.common.WorkerServiceGrpc.WorkerServiceStub
import proto.common._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Promise}

case class WorkerEntry(
                        host: String,
                        port: Int,
                        channel: ManagedChannel,
                        stub: WorkerServiceStub
                      )

/** App 이 아닌, 순수 상태/로직만 갖는 객체 */
object MasterState {
  implicit val ec: ExecutionContext = ExecutionContext.global

  // 실행 시 Master(App) 에서 세팅해 줄 값
  var numWorkers: Int = 0
  var host: String = "127.0.0.1"
  var port: Int = 8915

  val workers: mutable.ListBuffer[WorkerEntry] =
    mutable.ListBuffer.empty

  val whenRegisterCompleted: Promise[Unit] = Promise[Unit]()

  val ShuffleReadyFlags: mutable.Map[(String, Int), Boolean] =
    mutable.Map.empty

  val whenAllReadyToShuffle: Promise[Unit] = Promise[Unit]()

  val MergeReadyFlags: mutable.Map[(String, Int), Boolean] =
    mutable.Map.empty

  val whenAllReadyToMerge: Promise[Unit] = Promise[Unit]()
}
