package worker

import io.grpc.ManagedChannelBuilder
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

import proto.common._                  // WorkerData, WorkerDataResponse 등
import proto.common.MasterServiceGrpc  // gRPC 서비스 바인딩

import java.net.InetAddress
import scala.io.Source
import java.io.{File, PrintWriter, FileWriter, BufferedWriter}

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

  /* worker 기본 기능 구현 */

  /* Sort */

  // 1. 자기 자신의 IP 출력
  val ipAddress = InetAddress.getLocalHost.getHostAddress
  println(s"My IP: $ipAddress")

  val inputFile = "testinput"

  // 2. 첫 10줄 데이터 출력
  println("First 10 lines of testinput:")
  Source.fromFile(inputFile).getLines().take(10).foreach(println)

  val totalLines = 1000 // 충분히 클 경우: Source.fromFile(inputFile).getLines().length 등 사용 가능

  // 3~5. 커서로 100줄씩 읽어가며, 10번 반복 (1000줄 존재한다고 가정. 부족하면 끝까지)
  val chunkSize = 100
  var lineCursor = 0
  val lines = Source.fromFile(inputFile).getLines().toArray
  val output1Pw = new BufferedWriter(new FileWriter("testoutput_1")) // 누적 추가 쓰기
  val output2Pw = new BufferedWriter(new FileWriter("testoutput_2"))

  for (_ <- 1 to 10 if lineCursor < lines.length) {
    val chunk = lines.slice(lineCursor, Math.min(lineCursor + chunkSize, lines.length))
    val records = chunk.map { line =>
      if (line.length >= 100) (line.substring(0, 10), line.substring(10, 100))
      else (line.substring(0, Math.min(line.length, 10)), if (line.length > 10) line.substring(10) else "")
    }
    // 4. key 기준 정렬
    val sortedRecords = records.sortBy(_._1)
    // 5. "a000000000" 기준 분리
    val (file1, file2) = sortedRecords.partition(_._1 <= "a000000000")

    // 결과 파일에: 개수 + 데이터 누적 작성 (append)
    output1Pw.write(file1.length.toString + "\n")
    file1.foreach { case (k, v) => output1Pw.write(k + v + "\n") }

    output2Pw.write(file2.length.toString + "\n")
    file2.foreach { case (k, v) => output2Pw.write(k + v + "\n") }

    lineCursor += chunkSize
  }

  output1Pw.close()
  output2Pw.close()

  /* Merge */

  val mergedRecords = scala.collection.mutable.ListBuffer[(String, String)]()
  val src = Source.fromFile("testoutput_2")
  val iter = src.getLines()

  // testoutput_1에서 개수, 데이터 반복 파싱
  while (iter.hasNext) {
    val countLine = iter.next()
    val count = countLine.toInt
    for (_ <- 1 to count if iter.hasNext) {
      val line = iter.next()
      if (line.length >= 10) {
        mergedRecords += ((line.substring(0,10), line.substring(10)))
      } else {
        mergedRecords += ((line, ""))
      }
    }
  }
  src.close()

  // key 기준 전체 정렬
  val sortedRecords = mergedRecords.sortBy(_._1)

  // testoutput_final에 기록
  val pw = new PrintWriter(new File("testoutput_final"))
  sortedRecords.foreach { case (k, v) => pw.println(k + v) }
  pw.close()
}
