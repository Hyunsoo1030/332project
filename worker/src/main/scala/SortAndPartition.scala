package worker

import java.io._
import java.util.concurrent.{ExecutorService, Executors, Semaphore}
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import scala.collection.mutable.{ListBuffer, PriorityQueue}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.io.Source
import scala.util.{Try}

object SortAndPartition {

  private val pool: ExecutorService = Executors.newFixedThreadPool(4)
  implicit private val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)

  private val MaxConcurrentChunks: Int = 4
  private val chunkSemaphore = new Semaphore(MaxConcurrentChunks)

  private val ChunkSize: Int = 100000
  private val KeyLength: Int = 10

  private val TempPrefix = "sorted_chunk_"
  private val TempSuffix = ".tmp"

  // Sorting key: 앞 10바이트 기준
  private val lineOrdering: Ordering[String] =
    Ordering.by { line =>
      if (line.length >= KeyLength) line.substring(0, KeyLength)
      else line.padTo(KeyLength, ' ').mkString
    }

  // ------------------------------------------------------
  //                     CHUNK SORT
  // ------------------------------------------------------

  private def sortChunkAsync(lines: List[String]): Future[Path] = Future {
    val sorted = lines.sorted(lineOrdering)

    val tempPath = Files.createTempFile(TempPrefix, TempSuffix)
    Files.write(tempPath, sorted.mkString("\n").getBytes)

    tempPath
  }

  private def createSortedChunks(inputDirs: List[String]): Future[List[Path]] = Future {
    val chunkFutures = ListBuffer[Future[Path]]()
    val buffer = ListBuffer[String]()
    var totalInputSize: Long = 0

    inputDirs.foreach { dir =>
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).foreach { file =>
          totalInputSize += file.length()
          val br = Source.fromFile(file)

          for (line <- br.getLines()) {
            buffer += line

            if (buffer.size >= ChunkSize) {
              val chunk = buffer.toList
              buffer.clear()
              chunkFutures += sortChunkAsync(chunk)
            }
          }

          br.close()
        }
      }
    }
    println(s"[WORKER] Total input size = $totalInputSize bytes")

    // 마지막 chunk 처리
    if (buffer.nonEmpty) {
      chunkFutures += sortChunkAsync(buffer.toList)
    }

    Await.result(Future.sequence(chunkFutures.toList), Duration.Inf)
  }

  // ------------------------------------------------------
  //                     K-WAY MERGE
  // ------------------------------------------------------

  private case class Entry(line: String, idx: Int)
  private implicit val entryOrdering: Ordering[Entry] =
    Ordering.by(_.line.take(KeyLength))

  private def mergeSortedChunks(chunkFiles: List[Path]): Path = {
    if (chunkFiles.size == 1)
      return chunkFiles.head

    val readers = chunkFiles.map(p => Source.fromFile(p.toFile).getLines())

    val pq = PriorityQueue.empty[Entry](entryOrdering.reverse)

    // 초기 라인 로딩
    for ((it, idx) <- readers.zipWithIndex) {
      if (it.hasNext)
        pq.enqueue(Entry(it.next(), idx))
    }

    val out = Files.createTempFile("merged_", ".tmp")
    val bw = new BufferedWriter(new FileWriter(out.toFile))

    while (pq.nonEmpty) {
      val Entry(line, idx) = pq.dequeue()

      bw.write(line)
      bw.newLine()

      val it = readers(idx)
      if (it.hasNext)
        pq.enqueue(Entry(it.next(), idx))
    }

    bw.close()
    out
  }

  // fanIn 개씩 묶어서 단계적으로 파일 수를 줄임
  private def multiLevelMerge(files: List[Path], fanIn: Int = 32): Path = {
    var current = files

    while (current.size > 1) {
      val grouped = current.grouped(fanIn).toList

      val merged = grouped.map { group =>
        Future {
          mergeSortedChunks(group)
        }
      }

      current = Await.result(Future.sequence(merged), Duration.Inf)
    }

    current.head
  }

  // ------------------------------------------------------
  //                     PARTITION
  // ------------------------------------------------------

  // 각 partition의 시작 바이트 오프셋 반환
  private def findPartitionStartOffsets(sortedFile: Path, pivots: List[String]): List[Long] = {
    val starts = ListBuffer[Long]()
    starts += 0L

    if (pivots.isEmpty) return List(0L)

    val raf = new RandomAccessFile(sortedFile.toFile, "r")
    try {
      var currentPivotIdx = 0
      var line: String = null

      while ({
        val offsetBefore = raf.getFilePointer()
        val raw = raf.readLine()
        if (raw == null) {
          line = null
        } else {
          line = raw
        }

        if (line != null) {
          val key = if (line.length >= KeyLength) line.substring(0, KeyLength) else line.padTo(KeyLength, ' ').mkString
          while (currentPivotIdx < pivots.length && key > pivots(currentPivotIdx)) {
            starts += offsetBefore
            currentPivotIdx += 1
          }
        }

        line != null
      } ) ()

      while (starts.size < pivots.length + 1) {
        starts += raf.length()
      }
      println("[WORKER] Partition offset calculation was successful.")

      starts.toList
    } finally {
      raf.close()
    }
  }

  // 병렬로 각 partition file에 작성
  private def partitionSortedFile(sortedFile: Path, pivotList: List[String], myorder: Int): Unit = {
    val startOffsets = findPartitionStartOffsets(sortedFile, pivotList)
    val eof = Files.size(sortedFile)
    val ranges: Seq[(Long, Long)] = (0 until startOffsets.length).map { i =>
      val s = startOffsets(i)
      val e = if (i + 1 < startOffsets.length) startOffsets(i + 1) else eof
      (s, e)
    }

    val outputFiles: Seq[Path] = (0 to pivotList.length).map { i =>
      val path = Paths.get(s"partition_${myorder}_${i}.txt")
      Files.deleteIfExists(path)
      Files.createFile(path)
      path
    }

    val copyFutures = ranges.zipWithIndex.map { case ((s, e), idx) =>
      Future {
        if (e <= s) {
          // 빈 파티션: 아무것도 쓰지 않음
          ()
        } else {
          val in = new RandomAccessFile(sortedFile.toFile, "r")
          val out = new BufferedOutputStream(new FileOutputStream(outputFiles(idx).toFile, true))

          try {
            val buffer = new Array[Byte](64 * 1024) // 64KB
            var remaining = e - s
            in.seek(s)
            while (remaining > 0) {
              val toRead = if (remaining > buffer.length) buffer.length else remaining.toInt
              val read = in.read(buffer, 0, toRead)
              if (read <= 0) {
                remaining = 0
              } else {
                out.write(buffer, 0, read)
                remaining -= read
              }
            }
            out.flush()
          } finally {
            Try(in.close())
            Try(out.close())
          }
        }
      }
    }
    Await.result(Future.sequence(copyFutures.toList), Duration.Inf)

    println(s"[WORKER] Partition complete.")

    var totalSize: Long = 0
    outputFiles.zipWithIndex.foreach { case (path, idx) =>
      val size = Files.size(path)
      println(s"[WORKER] Partition_${myorder}_$idx.txt = $size bytes")
      totalSize += size
    }

    println(s"[WORKER] Total partition size = $totalSize bytes")
  }

  // ------------------------------------------------------
  //                     RUN
  // ------------------------------------------------------

  def run(inputDirs: List[String], pivotList: List[String], myorder: Int): Future[Unit] = {
    Future {
      println("[WORKER] Creating sorted chunks...")
      val chunks = Await.result(createSortedChunks(inputDirs), Duration.Inf)
      println(s"[WORKER] Created ${chunks.size} chunks.")

      println("[WORKER] K-way merging...")
      val merged = multiLevelMerge(chunks)
      println(s"[WORKER] Merge complete: $merged")

      println("[WORKER] Partitioning...")
      partitionSortedFile(merged, pivotList, myorder)
      Files.deleteIfExists(merged)

      println("[WORKER] Sort and partition complete.")
    }
  }
  // 1) 단일 파일에서 chunk들을 만드는 버전
  private def createSortedChunksFromFile(file: Path): Future[List[Path]] = Future {
    val chunkFutures = ListBuffer[Future[Path]]()
    val buffer = ListBuffer[String]()

    val br = Source.fromFile(file.toFile)
    try {
      for (line <- br.getLines()) {
        buffer += line
        if (buffer.size >= ChunkSize) {
          val chunk = buffer.toList
          buffer.clear()
          chunkFutures += sortChunkAsync(chunk)
        }
      }
    } finally {
      br.close()
    }

    if (buffer.nonEmpty) {
      chunkFutures += sortChunkAsync(buffer.toList)
    }

    Await.result(Future.sequence(chunkFutures.toList), Duration.Inf)
  }

  // 2) 단일 파일을 외부 정렬해서 outputPath로 저장하는 함수
  def externalSortFile(inputPath: Path, outputPath: Path): Unit = {
    println(s"[WORKER] externalSortFile: input=$inputPath output=$outputPath")

    val chunks = Await.result(createSortedChunksFromFile(inputPath), Duration.Inf)
    println(s"[WORKER] externalSortFile: created ${chunks.size} chunks")

    val merged = multiLevelMerge(chunks)
    println(s"[WORKER] externalSortFile: merged => $merged")

    // 최종 결과를 outputPath로 이동
    Files.deleteIfExists(outputPath)
    Files.move(merged, outputPath)
  }

}
