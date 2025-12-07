package worker

import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import proto.common._
import proto.common.{MasterServiceGrpc, WorkerServiceGrpc}

import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.io.RandomAccessFile
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._
import scala.util.{Random, Failure, Success}
import proto.common.WorkerServiceGrpc.WorkerServiceStub

import java.net.{Inet4Address, NetworkInterface}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Duration, DurationInt}


object Worker extends App {
  // 입력 요구 사항
  if (args.length < 4) {
    System.err.println("Usage: worker <masterIp:port> -I <inDir>... -O <outDir>")
    sys.exit(1)
  }

  // 1) 마스터 주소
  val masterAddr = args(0)
  val Array(masterHost, masterPortStr) =
    masterAddr.split(":", 2) match {
      case Array(h, p) => Array(h, p)
      case _ =>
        System.err.println("master address must be <host>:<port>")
        sys.exit(1)
    }
  val masterPort = masterPortStr.toInt

  // 2) -I 뒤의 input dirs (가변 개수)
  var idx = 1
  if (idx >= args.length || args(idx) != "-I") {
    System.err.println("Usage: worker <masterIp:port> -I <inDir>... -O <outDir>")
    sys.exit(1)
  }
  idx += 1

  import scala.collection.mutable.ListBuffer
  val inputDirs = ListBuffer.empty[String]

  // -O 나오기 전까지 전부 input dir
  while (idx < args.length && args(idx) != "-O") {
    inputDirs += args(idx)
    idx += 1
  }

  if (inputDirs.isEmpty) {
    System.err.println("At least one input directory is required after -I")
    sys.exit(1)
  }

  // 3) -O <outputDir> (무조건 하나)
  if (idx >= args.length || args(idx) != "-O" || idx + 1 >= args.length) {
    System.err.println("Usage: worker <masterIp:port> -I <inDir>... -O <outDir>")
    sys.exit(1)
  }
  val outputDir = args(idx + 1)

  implicit val ec: ExecutionContext = ExecutionContext.global

  /*로컬 파워셸 테스트용
  $env:MASTER_IP="localhost"
  $env:MASTER_PORT="8915"
  $env:WORKER_IP="localhost"
  $env:WORKER_PORT="8888"
  $env:DATA_PATH="C:\Users\matth\Desktop\cs332Project\332project\input"
  sbt worker/run
  sbt "worker/run localhost:8915 -I C:\Users\matth\Desktop\cs332Project\332project\test_input1 C:\Users\matth\Desktop\cs332Project\332project\test_input2 -O C:\Users\matth\Desktop\cs332Project\332project\test_output"
  */
  case class WorkerEntry
  (
    host: String,
    port: Int,
    order: Int,
    channel: io.grpc.ManagedChannel,
    stub: WorkerServiceStub
  )

  val workers: mutable.ListBuffer[WorkerEntry]
  = mutable.ListBuffer.empty[WorkerEntry]
  var pivotsList: List[String] = Nil
  var myOrder: Int = -1

  println(s"[WORKER] Connecting to master at $masterHost:$masterPort ...")

  val channelToMaster =
    ManagedChannelBuilder
      .forAddress(masterHost, masterPort)
      .usePlaintext() // TLS 안 씀
      .build()

  val masterStub: MasterServiceGrpc.MasterServiceStub =
    MasterServiceGrpc.stub(channelToMaster)

  println("[WORKER] set channel to master gRPC service.")

  //val masterIp    = sys.env("MASTER_IP")
  //val masterPort  = sys.env("MASTER_PORT").toInt
  //val dataPath    = sys.env("DATA_PATH")

  val workerHost    = findMyIp()

  val portF: Future[PortResponse] = masterStub.getNewPort(PortRequest())
  val portResp: PortResponse = Await.result(portF, Duration.Inf)
  val workerPort = portResp.portNum

  val server: Server =  //본인 서버 구축
    ServerBuilder
      .forPort(workerPort)
      .addService(WorkerServiceGrpc.bindService(new WorkerServiceImpl(inputDirs, outputDir), ec))
      .build()
      .start()

  println(s"[WORKER] gRPC server started, listening on $workerHost:$workerPort")

  masterStub.notifyConnection(ConnectionRequest(host=workerHost, port=workerPort))

  println("[WORKER] Registering with registerWorker RPC...")

  val registerResponseF: Future[WorkerDataResponse] =
    masterStub.registerWorker(WorkerData(workerHost=workerHost, workerPort=workerPort))

  registerResponseF.onComplete(_ -> {println("[WORKER] Registration completed.")})

  // Sort & Partition 완료 후 Master에게 신호 보내기
  val sortComplete: Promise[Unit] = Promise[Unit]()
  sortComplete.future.onComplete {
    case Success(_) =>
      println("[WORKER] Sort & Partition completed. Notifying master...")
      masterStub.readyToShuffle(ReadyRequest(host=workerHost, port=workerPort))
    case Failure(e) => println(s"[WORKER] Sort failed: ${e.getMessage}")
  }

  // shuffle 완료 후 Master에게 신호 보내기
  val shuffleComplete: Promise[Unit] = Promise[Unit]()
  shuffleComplete.future.onComplete{
    case Success(_) =>
      println("[WORKER] Shuffling completed. Notifying master...")
      masterStub.readyToMerge(ReadyRequest(host=workerHost, port=workerPort))
    case Failure(e) => println(s"[WORKER] Shuffling failed: ${e.getMessage}")
  }

  server.awaitTermination()


  def findMyIp(): String = {
    val nets = NetworkInterface.getNetworkInterfaces.asScala.toList

    val addrs =
      for {
        net <- nets
        if net.isUp && !net.isLoopback && !net.isVirtual
        addr <- net.getInetAddresses.asScala
        if addr.isInstanceOf[Inet4Address]
      } yield addr.getHostAddress

    addrs.headOption.getOrElse("127.0.0.1")
  }
}

class WorkerServiceImpl(inputDirs: ListBuffer[String], outputDir: String           // worker 로컬 데이터 파일 경로
                       )(implicit ec: ExecutionContext)
  extends WorkerServiceGrpc.WorkerService {

  private val sampleBytes: Long = 1024L * 1024L // 1MB

  // shuffle helper
  private def GetPartitionFile(srcOrder: Int, partitionIndex: Int): Path = {
    Paths.get(s"partition_${srcOrder}_${partitionIndex}.txt")
  }

  private def appendLinesToLocalFile(partitionIndex: Int,
                                     senderOrder: Int,
                                     lines: Seq[String]): Unit = {
    if (lines.isEmpty) return

    val path = Paths.get(outputDir, s"shuffled-part-$partitionIndex.out")

    val writer = Files.newBufferedWriter(
      path,
      StandardCharsets.ISO_8859_1,
      StandardOpenOption.CREATE,
      StandardOpenOption.APPEND
    )
    try {
      lines.foreach { line =>
        writer.write(line)
        writer.newLine()
      }
    } finally {
      writer.close()
    }
  }

  private def readChunkFromFile(file: Path,
                                chunkIndex: Int,
                                maxRecords: Int): (Seq[String], Boolean) = {
    val RecordSize = 100

    if (!Files.exists(file)) return (Seq.empty, false)

    val startOffset = chunkIndex.toLong * maxRecords * RecordSize
    val buf = ListBuffer[String]()

    val channel = Files.newByteChannel(file, StandardOpenOption.READ)

    try {
      channel.position(startOffset)

      val buffer = java.nio.ByteBuffer.allocate(RecordSize)

      var readCount = 0
      var bytesRead = channel.read(buffer)

      while (bytesRead == RecordSize && readCount < maxRecords) {
        buffer.flip()

        val arr = new Array[Byte](RecordSize)
        buffer.get(arr)

        // 그대로 문자열로 변환 (ISO_8859_1로 1:1 매핑)
        val record = new String(arr, StandardCharsets.ISO_8859_1)
        buf += record

        readCount += 1
        buffer.clear()

        bytesRead = channel.read(buffer)
      }

      // hasMore = 아직 파일 남아 있음 && 읽은 레코드가 maxRecords에 도달함
      val hasMore = bytesRead == RecordSize || channel.position() < Files.size(file)

      (buf.toList, hasMore)

    } finally {
      channel.close()
    }
  }




  override def shutdown(req: ShutdownRequest): Future[ShutdownResponse] = Future {
    println("[WORKER] Shutdown RPC received.")
    // 응답 객체
    ShutdownResponse()
  }.andThen { case _ =>
    Worker.server.shutdown()
    new Thread(() => System.exit(0)).start()}

  /** 디스크에서 1MB만 읽어서 Samples로 리턴 */
  override def getSamples(req: SampleRequest): Future[SampleResponse] = Future {
    println("[WORKER] getSamples called.")

    val RecordSize  = 100
    val KeySize     = 10

    // 0) inputDirs를 Path로 변환
    val dirPaths: ListBuffer[Path] = inputDirs.map(Paths.get(_)).to(ListBuffer)

    // 1) 모든 디렉토리에서 일반 파일만 모으기
    val files: Vector[Path] =
      dirPaths.flatMap { dir =>
        if (Files.exists(dir) && Files.isDirectory(dir)) {
          Files.list(dir).iterator().asScala
            .filter(p => Files.isRegularFile(p))
            .toVector
        } else {
          Vector.empty[Path]
        }
      }.toVector

    if (files.isEmpty) {
      println(s"[WORKER] inputDirs have no regular files: ${inputDirs.mkString(", ")}")
      SampleResponse(samples = Seq.empty)
    } else if (sampleBytes <= 0) {
      SampleResponse(samples = Seq.empty)
    } else {
      val numFiles = files.size
      val bytesPerFile: Long = sampleBytes / numFiles

      if (bytesPerFile < RecordSize) {
        println(s"[WORKER] bytesPerFile=$bytesPerFile < RecordSize; no samples.")
        SampleResponse(samples = Seq.empty)
      } else {
        val rnd = new Random()
        val samplesBuilder = Vector.newBuilder[String]

        files.foreach { file =>
          val fileSize = Files.size(file)
          if (fileSize >= RecordSize) {
            val usableBytes = math.min(bytesPerFile, fileSize)
            val numSamples  = (usableBytes / RecordSize).toInt

            if (numSamples > 0) {
              val maxOffset = fileSize - RecordSize

              val raf = new RandomAccessFile(file.toFile, "r")
              try {
                var i = 0
                while (i < numSamples) {
                  val rawOffset = math.abs(rnd.nextLong()) % (maxOffset + 1)
                  val alignedOffset = rawOffset - (rawOffset % RecordSize)

                  val buf = new Array[Byte](RecordSize)
                  raf.seek(alignedOffset)
                  raf.readFully(buf)

                  val keyBytes = java.util.Arrays.copyOfRange(buf, 0, KeySize)
                  val keyStr   = new String(keyBytes, StandardCharsets.ISO_8859_1)
                  samplesBuilder += keyStr

                  i += 1
                }
              } finally {
                raf.close()
              }
            }
          }
        }

        SampleResponse(samples = samplesBuilder.result())
      }
    }
  }

  override def sendPivots(req: PivotRequest): Future[PivotResponse] = Future {
    println("[WORKER] sendPivots called.") // TODO: 워커들 간 채널 구축하고 저장하기 - 본인 채널 안 열게 조심
    Worker.pivotsList = req.pivots.toList
    Worker.myOrder = req.myOrder
    var workerCount = 1

    println(s"[WORKER] Acquired pivots: ${Worker.pivotsList}")

    req.orderedWorkerData.filterNot(w => w.workerHost == Worker.workerHost && w.workerPort == Worker.workerPort)
      .foreach(w => {
        val channel = ManagedChannelBuilder
          .forAddress(w.workerHost, w.workerPort)
          .usePlaintext()
          .build()
        val stub = WorkerServiceGrpc.stub(channel)
        stub.notifyConnection(ConnectionRequest(host=Worker.workerHost, port=Worker.workerPort))
        Worker.workers += Worker.WorkerEntry(w.workerHost, w.workerPort, w.order, channel, stub)
        println(s"[WORKER] A channel with worker${w.order} registered at ${w.workerHost}:${w.workerPort}.")
        workerCount += 1
        if(workerCount == req.orderedWorkerData.length)
          println("[WORKER] All workers are registered.")
      })

    SortAndPartition.run(inputDirs.toList, Worker.pivotsList, Worker.myOrder).onComplete {
      case Success(_) =>
        println("[WORKER] success sort and partition.")
        Worker.sortComplete.success(())
      case Failure(e) =>
        println("[WORKER] fail sort and partition.")
        Worker.sortComplete.failure(e)
    }

    PivotResponse()
  }

  override def notifyConnection(req: ConnectionRequest): Future[ConnectionResponse] = Future {
    println(s"[WORKER] Connected from ${req.host}:${req.port}.")
    ConnectionResponse()
  }

  override def startShuffle(req: ShuffleRequest): Future[ShuffleResponse] = {
    val myOrder   = Worker.myOrder
    val chunkSize = 10000

    val myShufflePath = Paths.get(outputDir, s"shuffled-part-$myOrder.out")
    Files.deleteIfExists(myShufflePath)

    println(s"[WORKER-${Worker.myOrder}] startShuffle called for partition $myOrder.")

    // -------------------------------
    // 1) 자기 로컬 partition 먼저 읽기
    // -------------------------------
    def readLocalPartition(): Future[Unit] = Future {
      val localSrcOrder = Worker.myOrder
      var chunkIndex    = 0
      var cont          = true

      println(s"[WORKER-${Worker.myOrder}] reading local partition part-from-$localSrcOrder-$myOrder.out")

      while (cont) {
        val file = GetPartitionFile(localSrcOrder, myOrder)
        val (lines, hasMore) = readChunkFromFile(file, chunkIndex, chunkSize)

        if (lines.nonEmpty) {
          appendLinesToLocalFile(myOrder, localSrcOrder, lines)
        }

        cont = hasMore
        chunkIndex += 1
      }
    }

    // -------------------------------
    // 2) 한 워커에서 모든 chunk를 pull
    //    (재귀 + flatMap, 완전 비동기)
    // -------------------------------
    def pullFromWorker(w: Worker.WorkerEntry, chunkIndex: Int = 0): Future[Unit] = {
      val dataReq = DataRequest(
        partitionIndex = myOrder,
        chunkIndex     = chunkIndex,
        maxRecords     = chunkSize
      )

      w.stub.sendData(dataReq).flatMap { resp =>
        val lines = resp.payload.map(_.line)

        if (lines.nonEmpty) {
          appendLinesToLocalFile(myOrder, w.order, lines)
        }

        if (resp.hasMore) {
          // 다음 chunk 비동기로 이어붙임
          pullFromWorker(w, chunkIndex + 1)
        } else {
          Future.successful(())
        }
      }
    }

    // -------------------------------
    // 3) 모든 리모트 워커에서 동시에 pull
    // -------------------------------
    val sortedWorkers = Worker.workers.sortBy(_.order)

    val remotePullF: Future[Unit] =
      Future.sequence(sortedWorkers.map(w => pullFromWorker(w))).map(_ => ())

    // -------------------------------
    // 4) 전체 순서: 로컬 읽고 → 리모트들 pull
    //    (원하면 둘을 동시에 돌리고 싶으면 병렬로도 가능)
    // -------------------------------
    for {
      _ <- readLocalPartition()  // 로컬 먼저
      _ <- remotePullF           // 그 다음 리모트들에서 병렬로 pull
    } yield {
      println(s"[WORKER-$myOrder] Shuffle completed (async).")
      Worker.shuffleComplete.success(())
      ShuffleResponse()
    }
  }



  // --------------------------
  // sendData: partition 파일을 chunk 단위로 읽어 응답
  // --------------------------
  override def sendData(req: DataRequest): Future[DataResponse] = Future {
    val srcOrder       = Worker.myOrder        // 나(보내는 쪽)의 order
    val partitionIndex = req.partitionIndex    // 이 데이터를 받아갈 최종 워커
    val chunkIndex     = req.chunkIndex
    val maxRecords     = req.maxRecords

    val file = GetPartitionFile(srcOrder, partitionIndex)

    val (lines, hasMore) = readChunkFromFile(file, chunkIndex, maxRecords)

    DataResponse(
      payload = lines.map(line => Entity(line = line)),
      hasMore = hasMore
    )
  }

  override def startMerge(req: MergeRequest): Future[MergeResponse] = Future {
    val myOrder = Worker.myOrder
    val inputPath  = Paths.get(outputDir, s"shuffled-part-$myOrder.out")
    val outputPath = Paths.get(outputDir, s"final-$myOrder.txt")

    if (!Files.exists(inputPath)) {
      println(s"[WORKER-$myOrder] No shuffled file found: $inputPath")
      MergeResponse()
    } else {
      println(s"[WORKER-$myOrder] startMerge: external sort on $inputPath")
      SortAndPartition.externalSortFile(inputPath, outputPath)
      println(s"[WORKER-$myOrder] Merge completed → $outputPath")
      MergeResponse()
    }
  }

}