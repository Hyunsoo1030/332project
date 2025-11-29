import java.io._
import java.util.concurrent.{ExecutorService, Executors}
import java.util.concurrent.Semaphore

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.io.Source

object SortAndPartition {

  // 4-core 환경용 스레드풀
  private val pool: ExecutorService = Executors.newFixedThreadPool(4)
  implicit private val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)

  // OOM 방지 chunk 제한
  private val MaxConcurrentChunks: Int = 4  // 최대 동시 청크 처리 개수
  private val chunkSemaphore = new Semaphore(MaxConcurrentChunks)

  // 한 번에 메모리로 읽어 정렬할 라인 수 (필요에 맞게 조절)
  private val ChunkSize: Int = 100000

  // key: 각 라인 앞 10바이트
  private val KeyLength: Int = 10

  // 정렬 시 임시 파일 prefix
  private val TempPrefix = "sorted_chunk_"
  private val TempSuffix = ".tmp"
  // 전체 데이터 정렬 파일
  private val SortedOneFile = "sorted"

  // 정렬에 사용할 Ordering (key 기준)
  private val lineOrdering: Ordering[String] = Ordering.by { line: String =>
    if (line.length >= KeyLength) line.substring(0, KeyLength) else line.padTo(KeyLength, ' ')
  }

  // 외부에서 호출: inputFile -> 정렬된 outputFile 생성
  def workerSort(inputFile: String): Unit = {
    val outputFile = SortedOneFile
    println("[Worker] Sorting start")
    val chunkFiles = createSortedChunks(inputFile)
    try {
      kWayMerge(chunkFiles, outputFile)
    } finally {
      // 임시 청소
      chunkFiles.foreach(f => if (f.exists()) f.delete())
    }
    println("[Worker] Sorting complete")
  }

  // 외부에서 호출:
  //  - sortedFile : workerSort로 만들어진 전체 정렬 파일
  //  - numWorkers : 파티션할 worker 수
  //  - pivots     : 크기 numWorkers-1, 정렬된 pivot key 리스트
  // 결과:
  //   worker-0.out, worker-1.out, ..., worker-(numWorkers-1).out 생성
  def workerPartition(numWorkers: Int,
                      pivots: Seq[String],
                      outputPrefix: String = "worker-"): Unit = {

    require(pivots.size == numWorkers - 1, "[Worker] The number of pivots must be worker-1.")

    println("[Worker] Partitioning start")
    val sortedFile = SortedOneFile

    // 각 worker용 메모리 버퍼
    val buffers: Array[mutable.Buffer[String]] =
      Array.tabulate(numWorkers)(i => mutable.Buffer.empty[String])

    val src = Source.fromFile(sortedFile)
    try {
      val iter = src.getLines()

      // 청크 단위 처리
      while (iter.hasNext) {
        val chunk = iter.take(ChunkSize).toArray
        processChunk(chunk, buffers, pivots)

        // 버퍼가 일정 크기 이상 되면 즉시 디스크에 플러시
        if (buffers.exists(_.size > ChunkSize / numWorkers)) {
          flushBuffers(buffers, outputPrefix)
        }
      }

      // 마지막 버퍼 플러시
      flushBuffers(buffers, outputPrefix)
    } finally {
      src.close()
    }
    println("[Worker] Partitioning complete")
  }

  // ----------------------
  // 내부 구현부
  // ----------------------

  // inputFile을 ChunkSize 만큼 잘라 읽어 각 chunk를 정렬하고 임시 파일로 저장
  // 일부 chunk는 병렬로 처리
  private def createSortedChunks(inputFile: String): Seq[File] = {
    val src = Source.fromFile(inputFile)
    val chunkFiles = mutable.Buffer[File]()
    try {
      val iter = src.getLines()
      val futures = mutable.Buffer[Future[File]]()

      while (iter.hasNext) {
        val chunk = iter.take(ChunkSize).toArray
        val chunkId = chunkFiles.size + futures.size

        // Semaphore 획득 (동시성 제한)
        chunkSemaphore.acquire()

        val f = Future {
          try {
            val sorted = chunk.sorted(lineOrdering)
            val tmpFile = File.createTempFile(TempPrefix + chunkId + "_", TempSuffix)
            val bw = new BufferedWriter(new FileWriter(tmpFile))
            try {
              sorted.foreach { line =>
                bw.write(line)
                bw.newLine()
              }
            } finally {
              bw.close()
            }
            tmpFile
          } finally {
            chunkSemaphore.release()  // 작업 완료 후 반드시 해제
          }
        }

        futures += f
      }

      val all = Future.sequence(futures)
      val result = Await.result(all, Duration.Inf)
      chunkFiles ++= result
    } finally {
      src.close()
    }
    chunkFiles.toSeq
  }

  // k-way merge: 여러 정렬된 chunk 파일을 하나의 outputFile로 병합
  private def kWayMerge(chunkFiles: Seq[File], outputFile: String): Unit = {
    if (chunkFiles.isEmpty) {
      new File(outputFile).createNewFile()
      return
    }
    if (chunkFiles.size == 1) {
      // 단일 파일이면 그대로 copy
      copyFile(chunkFiles.head, new File(outputFile))
      return
    }

    val readers: Array[BufferedReader] =
      chunkFiles.map(f => new BufferedReader(new FileReader(f))).toArray

    try {
      val out = new BufferedWriter(new FileWriter(outputFile))
      try {
        // (현재 라인, 파일 index)를 담는 최소 힙
        implicit val heapOrd: Ordering[(String, Int)] =
          Ordering.by[(String, Int), String](_._1)(lineOrdering).reverse

        val pq = mutable.PriorityQueue[(String, Int)]()

        // 각 파일에서 첫 라인 읽어 삽입
        for (i <- readers.indices) {
          val line = readers(i).readLine()
          if (line != null) {
            pq.enqueue((line, i))
          }
        }

        // 힙에서 최소 key를 뽑아 출력하고, 해당 파일에서 다음 라인 읽어 다시 삽입
        while (pq.nonEmpty) {
          val (line, idx) = pq.dequeue()
          out.write(line)
          out.newLine()

          val next = readers(idx).readLine()
          if (next != null) {
            pq.enqueue((next, idx))
          }
        }
      } finally {
        out.close()
      }
    } finally {
      readers.foreach(_.close())
    }
  }

  private def processChunk(chunk: Array[String],
                           buffers: Array[mutable.Buffer[String]],
                           pivots: Seq[String]): Unit = {
    chunk.foreach { line =>
      val key = if (line.length >= KeyLength)
        line.substring(0, KeyLength)
      else
        line.padTo(KeyLength, ' ').mkString
      val workerIdx = findPartitionIndex(key, pivots)
      buffers(workerIdx) += line
    }
  }

  private def flushBuffers(buffers: Array[mutable.Buffer[String]],
                           prefix: String): Unit = {
    buffers.zipWithIndex.foreach { case (buffer, idx) =>
      if (buffer.nonEmpty) {
        val outputFile = new File(s"$prefix$idx")
        val writer = new BufferedWriter(new FileWriter(outputFile, true)) // append
        try {
          buffer.foreach { line =>
            writer.write(line)
            writer.newLine()  // 올바른 개행문자
          }
        } finally {
          writer.close()
          buffer.clear()
        }
      }
    }
  }

  // key가 어느 파티션에 속하는지 이진 탐색으로 결정
  // pivots 는 오름차순 정렬되어 있다고 가정
  private def findPartitionIndex(key: String, pivots: Seq[String]): Int = {
    var low = 0
    var high = pivots.length - 1
    var ans = pivots.length // 기본값: 마지막 파티션

    while (low <= high) {
      val mid = (low + high) >>> 1
      if (key <= pivots(mid)) {
        ans = mid
        high = mid - 1
      } else {
        low = mid + 1
      }
    }
    ans
  }

  private def copyFile(src: File, dst: File): Unit = {
    val in = new BufferedInputStream(new FileInputStream(src))
    try {
      val out = new BufferedOutputStream(new FileOutputStream(dst))
      try {
        val buf = new Array[Byte](1024 * 1024)
        Iterator
          .continually(in.read(buf))
          .takeWhile(_ != -1)
          .foreach(read => out.write(buf, 0, read))
      } finally {
        out.close()
      }
    } finally {
      in.close()
    }
  }

}