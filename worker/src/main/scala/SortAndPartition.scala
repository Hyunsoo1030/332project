import java.io._
import java.util.concurrent.{ExecutorService, Executors, Semaphore}

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.io.Source

object SortAndPartition {

  private val pool: ExecutorService = Executors.newFixedThreadPool(4)
  implicit private val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)

  private val MaxConcurrentChunks: Int = 4
  private val chunkSemaphore = new Semaphore(MaxConcurrentChunks)

  private val ChunkSize: Int = 100000
  private val KeyLength: Int = 10

  private val TempPrefix = "sorted_chunk_"
  private val TempSuffix = ".tmp"
  private val SortedOneFile = "sorted"

  // lineOrdering: 앞 10바이트 기준 정렬
  private val lineOrdering: Ordering[String] =
    Ordering.by { line =>
      if (line.length >= KeyLength) line.substring(0, KeyLength)
      else line.padTo(KeyLength, ' ').mkString
    }

  // ------------------------------------------------------
  // Worker: 입력 파일 전체 정렬
  // ------------------------------------------------------
  def workerSort(inputFiles: ListBuffer[String]): Unit = {
    println("[Worker] Sorting start")

    val allChunkFiles = mutable.Buffer[File]()
    inputFiles.foreach { filename =>
      allChunkFiles ++= createSortedChunks(filename)
    }

    try {
      kWayMerge(allChunkFiles.toSeq, SortedOneFile)
    } finally {
      allChunkFiles.foreach(f => if (f.exists()) f.delete())
    }

    println("[Worker] Sorting complete")
  }


  // ------------------------------------------------------
  // Worker: 파티션 분배
  // ------------------------------------------------------
  def workerPartition(numWorkers: Int,
                      pivots: Seq[String],
                      prefix: String = "worker-"): Unit = {

    require(pivots.size == numWorkers - 1)

    println("[Worker] Partitioning start")

    val src = Source.fromFile(SortedOneFile)
    val buffers = Array.fill(numWorkers)(mutable.Buffer[String]())

    try {
      val iter = src.getLines()

      while (iter.hasNext) {
        val chunk = iter.take(ChunkSize).toArray
        processChunk(chunk, buffers, pivots)

        if (buffers.exists(_.size > ChunkSize / numWorkers))
          flushBuffers(buffers, prefix)
      }

      flushBuffers(buffers, prefix)
    } finally {
      src.close()
    }

    println("[Worker] Partitioning complete")
  }


  // ------------------------------------------------------
  // Chunk 생성 + 정렬 (로컬 정렬)
  // ------------------------------------------------------
  private def createSortedChunks(inputFile: String): Seq[File] = {
    val src = Source.fromFile(inputFile)
    val futures = mutable.Buffer[Future[File]]()
    var chunkIndex = 0

    try {
      val iter = src.getLines()

      while (iter.hasNext) {
        val chunk = iter.take(ChunkSize).toArray
        if (chunk.nonEmpty) {

          val id = chunkIndex
          chunkIndex += 1

          chunkSemaphore.acquire()

          val f = Future {
            try {
              val sorted = chunk.sorted(lineOrdering)
              val tmpFile = File.createTempFile(TempPrefix + id + "_", TempSuffix)

              val bw = new BufferedWriter(new FileWriter(tmpFile))
              sorted.foreach { line =>
                bw.write(line); bw.newLine()
              }
              bw.close()

              tmpFile
            } finally {
              chunkSemaphore.release()
            }
          }

          futures += f
        }
      }

      // 안전한 배치 처리
      val result = mutable.Buffer[File]()
      futures.grouped(MaxConcurrentChunks).foreach { batch =>
        result ++= Await.result(Future.sequence(batch), Duration.Inf)
      }

      result.toSeq
    } finally {
      src.close()
    }
  }


  // ------------------------------------------------------
  // K-way Merge (단일 스레드)
  // ------------------------------------------------------
  private def kWayMerge(files: Seq[File], outName: String): Unit = {
    if (files.isEmpty) {
      new File(outName).createNewFile()
      return
    }
    if (files.size == 1) {
      copyFile(files.head, new File(outName))
      return
    }

    val readers = files.map(f => new BufferedReader(new FileReader(f))).toArray
    val out = new BufferedWriter(new FileWriter(outName))

    implicit val pqOrd: Ordering[(String, Int)] =
      Ordering.by[(String, Int), String](_._1)(lineOrdering).reverse

    val pq = mutable.PriorityQueue[(String, Int)]()

    try {
      for (i <- readers.indices) {
        val line = readers(i).readLine()
        if (line != null) pq.enqueue((line, i))
      }

      while (pq.nonEmpty) {
        val (line, idx) = pq.dequeue()
        out.write(line); out.newLine()

        val next = readers(idx).readLine()
        if (next != null) pq.enqueue((next, idx))
      }

    } finally {
      out.close()
      readers.foreach(_.close())
    }
  }


  // ------------------------------------------------------
  // 파티션 분배 로직
  // ------------------------------------------------------
  private def processChunk(chunk: Array[String],
                           buffers: Array[mutable.Buffer[String]],
                           pivots: Seq[String]): Unit = {

    val last = pivots.length

    chunk.foreach { line =>
      val key =
        if (line.length >= KeyLength) line.substring(0, KeyLength)
        else line.padTo(KeyLength, ' ').mkString

      // 올바른 문자열 비교
      var idx = 0
      while (idx < last && key.compareTo(pivots(idx)) > 0) idx += 1

      buffers(idx) += line
    }
  }

  private def flushBuffers(buffers: Array[mutable.Buffer[String]],
                           prefix: String): Unit = {

    buffers.zipWithIndex.foreach { case (buf, idx) =>
      if (buf.nonEmpty) {
        val file = new File(s"$prefix$idx.out")
        val bw = new BufferedWriter(new FileWriter(file, true))

        buf.foreach { line =>
          bw.write(line); bw.newLine()
        }

        bw.close()
        buf.clear()
      }
    }
  }


  // ------------------------------------------------------
  // 파일 복사
  // ------------------------------------------------------
  private def copyFile(src: File, dst: File): Unit = {
    val in = new BufferedInputStream(new FileInputStream(src))
    val out = new BufferedOutputStream(new FileOutputStream(dst))

    val buf = new Array[Byte](1024 * 1024)
    Iterator.continually(in.read(buf)).takeWhile(_ != -1)
      .foreach(n => out.write(buf, 0, n))

    in.close()
    out.close()
  }
}
