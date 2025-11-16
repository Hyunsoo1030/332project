# 🗓 팀프로젝트 회의 기록 (5주차-2)

---

### 회의 기본 정보

- **주차:** 6주차
- **회의 일시:** 2025.11.16 19:00
- **참여자:** 박민혁, 이현수, 이준엽
- **회의 방식:** 대면

---

### 프로젝트 진행 및 Milestone 점검

- [x]  현수 - 프로그램 작동 기반 gRPC 구현
- ZIO gRPC tutorial 기반으로 프로젝트 스켈레톤 작성 완료
- 라이브러리 버전 통합에서 어려움을 겪었지만, master와 worker간 통신 가능한 상태


![image.png](/Users/hyunsoo13/Desktop/2025/software design methods/332project/report/image/master_test.png)

![image.png](/Users/hyunsoo13/Desktop/2025/software design methods/332project/report/image/worker_test.png)

- 이제 도커 환경까지 구축해서 실제 클러스터 master - worker 처럼 만들 계획
- problem definition은 피피티에 도식화해놓은 상태
- exception handler처럼 master-worker 관계를 도식화해서 피피티에 추가할 계획
- master 코드 담당이었던 만큼 master 코드에서 어떤 기능을 구현할 지도 도식화할 계획

- [x]  준엽 -  Worker 돌아가게끔 코드 작성
    - gRPC 부분을 제외하고 기본적으로 돌아가는 코드 구현
        1. 본인의 ip 출력
        2. 파일에서 데이터 원하는 만큼 읽어오기
        3. key / value mapping
        4. 정렬 후 다시 파일에 기록
    
    ```scala
    import java.net.InetAddress
    import java.io.{FileInputStream, BufferedInputStream, PrintWriter, File}
    import scala.collection.mutable.ArrayBuffer
    
    object Worker {
      def main(args: Array[String]): Unit = {
    
        // 1. 본인의 IP 주소 출력
        val ip = InetAddress.getLocalHost.getHostAddress
        println(s"My IP Address: $ip")
    
        // 2. testinput 파일에서 10줄 읽기 (binary file 기반)
        val filename = "testinput"
        val fis = new BufferedInputStream(new FileInputStream(filename))
    
        // 바이너리에서 한 줄씩 읽기 (\n 기준)
        def readLineFromBinary(in: BufferedInputStream): Option[Array[Byte]] = {
          val buffer = new ArrayBuffer[Byte]()
          var b = in.read()
    
          if (b == -1) return None
    
          while (b != -1 && b != '\n') {
            buffer += b.toByte
            b = in.read()
          }
          Some(buffer.toArray)
        }
    
        // 10줄 읽기
        val lines = new ArrayBuffer[Array[Byte]]()
        var count = 0
        while (count < 10) {
          readLineFromBinary(fis) match {
            case Some(bytes) =>
              lines += bytes
              count += 1
            case None =>
              sys.error("testinput 파일에 줄이 10개 이하입니다.")
          }
        }
        fis.close()
    
        // 3. key/value 변환 (앞 10바이트 = key)
        val kvPairs = lines.map { line =>
          val key = line.slice(0, 10)
          val value = line.slice(10, 100)
          (key, value)
        }
    
        println("send 10 lines to master")
    
        // 4. key 기준 정렬하여 testoutput 작성
        //val sorted = kvPairs.sortBy(_._1)  // key 기준
        val sorted = kvPairs.sortBy { case (k, _) => new String(k, "ISO-8859-1") }
    
        val outFile = new File("testoutput")
        val writer = new PrintWriter(outFile)
    
        writer.write(sorted.size) // 10
        writer.write('\n')
    
        sorted.foreach { case (k, v) =>
          writer.write(k)
          writer.write(v)
          writer.write('\n')
        }
        writer.close()
    
        println("Done. Written 10 lines to testoutput.")
      }
    }
    ```
    
- [ ]  민혁 - Problem documentation

### 회의 내용 요약

**프로젝트 진행도 및 Milestone 점검**

- ZIO gRPC 활용

**개별 논의 주제**

- 

**주요 논의 주제**

- 발표
    - 한국어로 발표
    - master worker gRPC가 어떻게 되어있는지
    - 어떻게 흘러가는 프로그램인지
    - 문제 설명은 간략히 - 만들어둔 프레젠테이션 한 장으로도 충분할 듯
    - 어떻게 팀워킹해왔는지도
    - 각자 맡은 역할 3분할로 발표 자료 준비해오기
- 추후 프로젝트 진행
    - disk overflow가 발생하는 경우는 어떻게 대처할 것인가
    - shuffle의 경우 gRPC를 통해 버퍼의 데이터를 넘겨주기 전에 space가 충분한 지 물어본다
    - 다른 단계의 경우도 더 생각해볼 것 - 추후에 추가적인 edge-case로 test
    - protobuf 메시지를 어느 정도까지 정해보는 게 좋을 지

---

### 결정 사항

- Scala version 2.13.16으로 통일
- 깃허브 레포 변경 /cs332Project → /332Project

---

### 다음 주 Milestone / To-Do

> sorting은 가능하게 해놓기
> 
- 현수
    - [ ]  docker 환경 구축
    - [ ]  master 기능 구현
    - [ ]  발표 자료 만들어서 디코에 올리기
- 준엽
    - [ ]  worker 기능 구현
    - [ ]  worker에 gRPC 연결
    - [ ]  발표 자료 만들어서 디코에 올리기
- 민혁
    - [ ]  작성한 코드 버전 맞춰서 테스트 해보고 결과 공유하기
    - [ ]  협업 방법, 프로젝트 개요 발표 자료 준비
    - [ ]  발표자료 수합해서 깃허브에 커밋

### 

###