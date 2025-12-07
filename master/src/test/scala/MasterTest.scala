package master

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._

import proto.common._
import proto.common.MasterServiceGrpc

import Master._

/** 테스트용 Master 상태 */
final case class TestMasterState(
                                  var nextPort: Int = 50500,
                                  shuffleReadyFlags: mutable.Map[(String, Int), Boolean] = mutable.Map.empty,
                                  mergeReadyFlags:   mutable.Map[(String, Int), Boolean] = mutable.Map.empty,
                                  whenAllReadyToShuffle: Promise[Unit] = Promise[Unit](),
                                  whenAllReadyToMerge:   Promise[Unit] = Promise[Unit]()
                                )

class MasterServiceImplSpec extends AnyFunSuite {

  implicit val ec: ExecutionContext = ExecutionContext.global

  /**
   * 실제 MasterServiceImpl 의 로직을 거의 그대로 옮겨온
   * "테스트용 MasterServiceImpl".
   * (Master 객체 대신 TestMasterState 를 참조하도록 바꾼 버전)
   */
  class TestMasterServiceImpl(state: TestMasterState)(implicit ec: ExecutionContext)
    extends MasterServiceGrpc.MasterService {

    // === getNewPort ===
    override def getNewPort(req: PortRequest): Future[PortResponse] = Future {
      val p = this.synchronized {
        val cur = state.nextPort
        state.nextPort += 1
        cur
      }
      PortResponse(portNum = p)
    }

    // === registerWorker ===
    // 이 테스트에서는 네트워크 연결까지는 안 보므로, 최소 구현만 둡니다.
    override def registerWorker(req: WorkerData): Future[WorkerDataResponse] =
      Future.successful(WorkerDataResponse())

    override def notifyConnection(req: ConnectionRequest): Future[ConnectionResponse] =
      Future.successful(ConnectionResponse())

    // === readyToShuffle ===
    override def readyToShuffle(req: ReadyRequest): Future[ReadyResponse] = Future {
      val key = (req.host, req.port)

      if (!state.shuffleReadyFlags.contains(key)) {
        state.shuffleReadyFlags += (key -> false)
      }

      state.shuffleReadyFlags.update(key, true)
      println(s"[TEST] Worker at ${req.host}:${req.port} is ready to shuffle.")

      val allReady = state.shuffleReadyFlags.values.forall(_ == true)
      if (allReady && !state.whenAllReadyToShuffle.isCompleted) {
        println("[TEST] All workers are ready to shuffle!")
        state.whenAllReadyToShuffle.trySuccess(())
      }

      ReadyResponse()
    }

    // === readyToMerge ===
    override def readyToMerge(req: ReadyRequest): Future[ReadyResponse] = Future {
      val key = (req.host, req.port)

      if (!state.mergeReadyFlags.contains(key)) {
        state.mergeReadyFlags += (key -> false)
      }

      state.mergeReadyFlags.update(key, true)
      println(s"[TEST] Worker at ${req.host}:${req.port} is ready to merge.")

      val allReady = state.mergeReadyFlags.values.forall(_ == true)
      if (allReady && !state.whenAllReadyToMerge.isCompleted) {
        println("[TEST] All workers are ready to merge!")
        state.whenAllReadyToMerge.trySuccess(())
      }

      ReadyResponse()
    }
  }

  // ---------------------------------------------------------------------------
  // 실제 테스트들
  // ---------------------------------------------------------------------------

  test("getNewPort should return increasing port numbers") {
    val state  = TestMasterState(nextPort = 50500)
    val service = new TestMasterServiceImpl(state)

    val p1 = Await.result(service.getNewPort(PortRequest()), 1.second)
    val p2 = Await.result(service.getNewPort(PortRequest()), 1.second)
    val p3 = Await.result(service.getNewPort(PortRequest()), 1.second)

    assert(p1.portNum == 50500)
    assert(p2.portNum == 50501)
    assert(p3.portNum == 50502)
  }

  test("readyToShuffle should set flags and complete whenAllReadyToShuffle when all workers are ready") {
    val state = TestMasterState()
    val service = new TestMasterServiceImpl(state)

    // 테스트용 worker 3개를 미리 등록해 놓는다 (플래그는 false)
    val workers = Seq(
      ("10.0.0.1", 50500),
      ("10.0.0.2", 50501),
      ("10.0.0.3", 50502)
    )

    workers.foreach { case (h, p) =>
      state.shuffleReadyFlags += ((h, p) -> false)
    }

    // 3개 다 readyToShuffle 호출
    workers.foreach { case (h, p) =>
      Await.result(service.readyToShuffle(ReadyRequest(host = h, port = p)), 1.second)
    }

    // 모든 플래그가 true 인지 확인
    assert(state.shuffleReadyFlags.values.forall(_ == true))

    // whenAllReadyToShuffle 가 complete 되었는지 확인
    val completed =
      Await.result(state.whenAllReadyToShuffle.future.map(_ => true).recover(_ => false), 1.second)

    assert(completed, "whenAllReadyToShuffle should be completed when all workers are ready")
  }

  test("readyToMerge should set flags and complete whenAllReadyToMerge when all workers are ready") {
    val state = TestMasterState()
    val service = new TestMasterServiceImpl(state)

    val workers = Seq(
      ("10.0.0.1", 50500),
      ("10.0.0.2", 50501)
    )

    workers.foreach { case (h, p) =>
      state.mergeReadyFlags += ((h, p) -> false)
    }

    workers.foreach { case (h, p) =>
      Await.result(service.readyToMerge(ReadyRequest(host = h, port = p)), 1.second)
    }

    assert(state.mergeReadyFlags.values.forall(_ == true))

    val completed =
      Await.result(state.whenAllReadyToMerge.future.map(_ => true).recover(_ => false), 1.second)

    assert(completed, "whenAllReadyToMerge should be completed when all workers are ready")
  }
}
