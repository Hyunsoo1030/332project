package worker

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

import java.nio.file.{Files, Path, StandardOpenOption}
import java.nio.charset.StandardCharsets

import proto.common._

class WorkerServiceImplSpec extends AnyFunSuite {

  implicit val ec: ExecutionContext = ExecutionContext.global

  /** 임시 디렉토리 하나 만들어서 넘겨주는 헬퍼 */
  private def withTempDir(testCode: Path => Any): Unit = {
    val dir = Files.createTempDirectory("worker-test-")
    try {
      testCode(dir)
    } finally {
      // 필요하면 여기서 디렉토리/파일 정리 로직 추가 가능
      ()
    }
  }

  test("getSamples - no regular files -> empty samples") {
    withTempDir { inputDir =>
      val outputDir = Files.createTempDirectory("worker-out-")

      // inputDir 안에 파일을 하나도 안 만든 상태
      val inputDirs = ListBuffer(inputDir.toString)

      val service = new WorkerServiceImpl(
        inputDirs  = inputDirs,
        outputDir  = outputDir.toString
      )

      val resp = Await.result(service.getSamples(SampleRequest()), 2.seconds)

      assert(resp.samples.isEmpty)
    }
  }

  test("getSamples - fixed-length file -> extract keys") {
    withTempDir { inputDir =>
      val outputDir = Files.createTempDirectory("worker-out-")

      val recordSize = 100  // WorkerServiceImpl 내부 RecordSize와 맞춤
      val keySize    = 10   // WorkerServiceImpl 내부 KeySize와 맞춤

      val filePath = inputDir.resolve("data.bin")

      // 더미 레코드 10개 생성
      val numRecords = 10
      val oneRecord: Array[Byte] = {
        val arr = new Array[Byte](recordSize)
        val keyBytes = "KEY0000000".getBytes(StandardCharsets.ISO_8859_1) // 길이 10
        System.arraycopy(keyBytes, 0, arr, 0, keySize)
        // 나머지 바이트는 0으로 둠
        arr
      }

      val allBytes = Array.fill(numRecords)(oneRecord).flatten

      Files.write(
        filePath,
        allBytes,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING
      )

      val inputDirs = ListBuffer(inputDir.toString)
      val service   = new WorkerServiceImpl(inputDirs, outputDir.toString)

      val resp = Await.result(service.getSamples(SampleRequest()), 2.seconds)

      // 1) 비어 있으면 안 됨
      assert(resp.samples.nonEmpty)

      // 2) 각 키의 길이는 10이어야 함
      assert(resp.samples.forall(_.length == keySize))

      // 3) 우리가 넣어둔 key 값이 적어도 하나는 나와야 함
      assert(resp.samples.exists(_ == "KEY0000000"))
    }
  }
}
