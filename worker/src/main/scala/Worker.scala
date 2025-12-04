package worker

import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}

import scala.concurrent.{Await, ExecutionContext, Future}
import proto.common._
import proto.common.{MasterServiceGrpc, WorkerServiceGrpc}

import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.io.RandomAccessFile
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._
import scala.util.Random
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

class WorkerServiceImpl(inputDirs: ListBuffer[String],   // worker 로컬 데이터 파일 경로
                        outputDir: String)               // shuffle 결과/중간 파일을 쓸 디렉토리
                       (implicit ec: ExecutionContext)
  extends WorkerServiceGrpc.WorkerService {

  private val sampleBytes: Long = 1024L * 1024L // 1MB

  // ===========================
  // Shuffle용 헬퍼 함수들
  // ===========================

  /** 파티션 파일 경로
   * 정렬+파티션 단계에서 prefix를 s"$outputDir/part-from-${Worker.myOrder}-" 로 쓴다고 가정:
   *   => outputDir/part-from-<srcOrder>-<partitionIndex>.out
   */
  private def partitionFile(srcOrder: Int, partitionIndex: Int): Path = {
    Paths.get(outputDir, s"part-from-$srcOrder-$partitionIndex.out")
  }

  /** 최종 shuffle 결과 파일에 append
   * 한 파티션(index)에 대한 모든 데이터는 "shuffled-part-<partitionIndex>.out" 로 모은다.
   */
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

  /** partition 파일에서 (chunkIndex * maxRecords)번째 줄부터 maxRecords 줄을 읽어온다.
   * 반환값: (읽어온 라인들, 아직 더 남아 있는지 여부)
   * 단순 O(n) 구현이라 느릴 수 있지만, 로컬 테스트용으로는 충분.
   */
  private def readChunkFromFile(file: Path,
                                chunkIndex: Long,
                                maxRecords: Int): (Seq[String], Boolean) = {
    if (!Files.exists(file)) return (Seq.empty, false)

    val startLine = chunkIndex * maxRecords
    val reader = Files.newBufferedReader(file, StandardCharsets.ISO_8859_1)

    val buf = ListBuffer[String]()
    var lineNum = 0L
    var eof = false
    var hitChunkLimit = false

    try {
      var line: String = null
      while (!eof && !hitChunkLimit) {
        line = reader.readLine()
        if (line == null) {
          eof = true
        } else {
          if (lineNum >= startLine && buf.size < maxRecords) {
            buf += line
          }
          lineNum += 1
          if (buf.size >= maxRecords) {
            hitChunkLimit = true
          }
        }
      }
    } finally {
      reader.close()
    }

    val hasMore = !eof && hitChunkLimit
    (buf.toList, hasMore)
  }

  // ===========================
  // RPC 구현들
  // ===========================

  override def shutdown(req: ShutdownRequest): Future[ShutdownResponse] = Future {
    println("[WORKER] Shutdown RPC received.")
    // 응답 객체
    ShutdownResponse()
  }.andThen { case _ =>
    Worker.server.shutdown()
    new Thread(() => System.exit(0)).start()
  }

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
    Worker.myOrder    = req.myOrder
    var workerCount   = 1

    println(s"[WORKER] Acquired pivots: ${Worker.pivotsList}")

    req.orderedWorkerData
      .filterNot(w => w.workerHost == Worker.workerHost && w.workerPort == Worker.workerPort)
      .foreach { w =>
        val channel = ManagedChannelBuilder
          .forAddress(w.workerHost, w.workerPort)
          .usePlaintext()
          .build()
        val stub = WorkerServiceGrpc.stub(channel)
        stub.notifyConnection(ConnectionRequest(host = Worker.workerHost, port = Worker.workerPort))
        Worker.workers += Worker.WorkerEntry(w.workerHost, w.workerPort, w.order, channel, stub)
        println(s"[WORKER] A channel with worker${w.order} registered at ${w.workerHost}:${w.workerPort}.")
        workerCount += 1
        if (workerCount == req.orderedWorkerData.length)
          println("[WORKER] All workers are registered.")
      }

    PivotResponse()
  }

  override def notifyConnection(req: ConnectionRequest): Future[ConnectionResponse] = Future {
    println(s"[WORKER] Connected from ${req.host}:${req.port}.")
    ConnectionResponse()
  }

  // ===========================
  // startShuffle: pull 기반 shuffle
  // ===========================
  override def startShuffle(req: ShuffleRequest): Future[ShuffleResponse] = Future {
    // 이 워커가 최종적으로 담당할 파티션 index
    // (마스터가 workerNumber == myOrder 로 보내준다고 가정)
    val myPartition = req.workerNumber
    val chunkSize   = 10000   // 한 번에 몇 줄씩 받을지 (필요하면 조정)

    println(s"[WORKER-$myPartition] startShuffle called.")

    // 1) 자기 자신이 가진 파티션 (from myOrder -> myPartition)은 로컬에서 바로 복사
    {
      val srcOrder   = Worker.myOrder
      val localFile  = partitionFile(srcOrder, myPartition)
      var chunkIndex = 0L
      var cont       = true

      while (cont) {
        val (lines, hasMore) = readChunkFromFile(localFile, chunkIndex, chunkSize)
        if (lines.nonEmpty) {
          appendLinesToLocalFile(myPartition, srcOrder, lines)
        }
        cont = hasMore
        chunkIndex += 1
      }
      println(s"[WORKER-$myPartition] Local partition copied.")
    }

    // 2) 다른 워커들로부터 pull
    val sortedWorkers = Worker.workers.sortBy(_.order)

    sortedWorkers.foreach { w =>
      println(s"[WORKER-$myPartition] Pulling from worker${w.order} ...")

      var chunkIndex = 0
      var cont       = true

      while (cont) {
        val dataReq = DataRequest(
          partitionIndex = myPartition,
          chunkIndex     = chunkIndex,
          maxRecords     = chunkSize
        )

        val resp = Await.result(w.stub.sendData(dataReq), 30.seconds)

        if (resp.payload.nonEmpty) {
          // head + body 다시 합쳐서 한 줄로 복원
          val lines = resp.payload.map(e => e.head + e.body)
          appendLinesToLocalFile(myPartition, w.order, lines)
        }

        cont = resp.hasMore
        chunkIndex += 1
      }
    }

    println(s"[WORKER-$myPartition] Shuffle completed.")
    ShuffleResponse()
  }

  // ===========================
  // sendData: partition 파일을 chunk 단위로 읽어 응답
  // ===========================
  override def sendData(req: DataRequest): Future[DataResponse] = Future {
    val srcOrder       = Worker.myOrder         // 나(보내는 쪽)의 order
    val partitionIndex = req.partitionIndex     // 이 데이터를 받아갈 최종 워커의 파티션 index
    val chunkIndex     = req.chunkIndex.toLong
    val maxRecords     = req.maxRecords

    val file = partitionFile(srcOrder, partitionIndex)

    val (lines, hasMore) = readChunkFromFile(file, chunkIndex, maxRecords)

    // 한 줄을 (head, body)로 쪼개서 Entity로 포장
    val entities = lines.map { line =>
      val headLen = 10
      val head =
        if (line.length >= headLen) line.substring(0, headLen)
        else line.padTo(headLen, ' ')
      val body =
        if (line.length > headLen) line.substring(headLen)
        else ""
      Entity(head = head, body = body)
    }

    DataResponse(
      payload = entities,
      hasMore = hasMore
    )
  }

  override def startMerge(req: MergeRequest): Future[MergeResponse] = Future {
    println(s"[WORKER-${Worker.myOrder}] startMerge called (TODO: implement final merge).")
    // TODO: outputDir/shuffled-part-<partition>.out 들을
    //       최종 결과 파일로 머지하는 로직을 여기서 구현.
    MergeResponse()
  }
}


