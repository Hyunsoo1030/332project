# 332 project (week 3)

### 🧩 회의 기본 정보

- **주차:** 3주차
- **회의 일시: November 2, 2025**
- **참여자:** (이현수, 이준엽, 박민혁)
- **회의 방식:** (**대면** / 비대면 / 혼합)

---

### 🎯 개별 프로젝트 진전도

> 회의 전에 의논되었거나, 이번 논의때 다뤄보면 좋을 것들
> 
- [x]  discord/notion 환경 구축 완료(@이현수(무은재학부))
- [x]  프로젝트 문제 상황 파악(@이현수(무은재학부), @박민혁, @이준엽)
- [ ]  docker 설정(ongoing)

---

### 💬 주요 논의 주제

> 팀 협업 방식
> 
> 
> 문제 재정의 및 세부 사항 결정
> 
> 개인별 역할 분배
> 

---

### 🧠 회의 내용 요약

1. 팀 협업 방식

민혁: 주간 회의 기록 형식과 개별 일일 보고서 형식 제안, 노션에 개별 이름 밑에 구조화하여 보고서 정리

민혁: 팀프로젝트 관련 작업 시 디스코드 음성 채널에 mute상태로 참여하여 빠른 소통 가능하게 하기

현수: 채팅 채널을 세분화하여 개별 학습 내용 정리나 아이디어 제안 등에 사용

민혁: 수술팀 구조 도입 - 팀장

준엽: 수술팀 내에서 각자의 역할도 확실하게 정해서 커뮤니케이션 부하 최대한 줄이기

현수: 2명은 코드 작성, 1명은 그 system을 crash할 수 있는 test 최대한 고민해보기

현수: 클러스터 계정 받는대로 docker 환경 구축해보기

민혁: distribute sort? 조사해서 적용시켜보기

현수: notion comment 기능 통해 개별 daily progress report 평가

민혁: DPR은 매일 올리고, 올리기 전에 notion comment 남기기

민혁: 각자 프로젝트 관련 아이디어 있으면 LLM 링크도 노션에 공유해보기

준엽: 위 링크 같은 내용들 Daily Progress Report의 Action Repuest 같은 부분에 올려보기

---

1. 문제 재정의 및 세부 사항 결정

**데이터**

- 100bytes - 10bytes key / 90bytes data
- use only key when sorting
- 앞 byte부터 compare
- KeyA < KeyB if:
    - KeyA[i] == KeyB[i] for all i < k
    - KeyA[k] < KeyB[k] for some k < 10

**제약사항**

- The input data does not fit in **memory.** (memory < input data)
    
    → **disk-based merge sort**
    
- The input data does not fit in **disk.** (disk < input data)
→ distributed sorting
- 클러스터 머신 사양 by Scala 7 ppt
    - Machine: CPU 4 / RAM 24GB / Disk 500GB
    - Java 8 and SBT 1.11.7 are pre-installed.
    - install new programs **in your home directory**
    - 데이터는 worker 안에 미리 존재, gensort는 클러스터 외부 테스트 환경에서 사용

**동작 순서**

1. Initialize
- 
1. Sampling
- 목적: worker node에 최대한 균일하게 데이터가 들어가게끔 하는 것
    - 각 disk 별로 50 ~ 100 MB 정도 추출
        - 나중에 유연성까지 고려했을 때, 추출하는 데이터들이 마스터노드 디스크 범위를 벗어나지 않도록 추가적인 고민 필요
        - 적합성과 overhead 사이 point를 찾아야 함
    - Master node에 가져와서 정렬 후 Worker 별로 Partition range를 할당
1. Sort/Partition
- sorting algorithm 공부
- 메모리 사이즈 단위로 데이터 Sort
    - Sorting algorithm 정하기
- Worker 수만큼 각 memory-sized data 분할(Partition)
    - Partition method 구체화
1. Shuffle
- partition된 memory-sized data를 sampling 과정에서 각 worker에 할당된 range에 맞게 데이터를 주고받는 과정
- gRPC 를 통해 overhead가 적은 방향으로 동작할 수 있게끔
- worker node 마다 data send unit과 receive unit 필요
1. Merge
- 각 worker node 내에서 merge sort
    - sampling과 shuffling의 범위가 정확히 일치하지 않을 경우 발생하는 overflowed value에 대한 처리방법

현수: 일단은 너무 edge-case까지 고려하지 말고 동작가능하게 구현하는 걸 우선으로 - 기본적인 프로젝트 algorithm 우선

---

### 📍 다음 주 Milestone / To-Do

> 다음 주(4주차)까지 수행해야 할 과제, 테스트, 구현 목표 등
> 
1. 공통
- [ ]  클러스터 접속해서 command 확인

![image.png](332%20project%20(week%203)/image.png)

![image.png](332%20project%20(week%203)/image%201.png)

1. 개인
- 이현수
    - [ ]  docker 환경 구축 완료
    - [ ]  sampling
        - [ ]  algorithm documentation
        - [ ]  write pseudocode
- 이준엽
    - [ ]  sort
        - [ ]  algorithm documentation
        - [ ]  write pseudocode
    - [ ]  partition
        - [ ]  algorithm documentation
        - [ ]  write pseudocode
- 박민혁
    - [ ]  shuffle
        - [ ]  algorithm documentation
        - [ ]  write pseudocode
    - [ ]  merge
        - [ ]  algorithm documentation
        - [ ]  write pseudocode
    - [ ]  workspace 정리 (notion, github, discord)
        - [ ]  Notion main page 정리
        - [ ]  Daily report, weekly report template 생성
        - [ ]  Discord 채널 세분화
- [ ]  (optional) multithreading 관련 공부 + 아이디어 내오기

---

### ⚙️ 추가 논의 필요사항

> 회의 내에서 해결되지 않았거나 추가 검토가 필요한 주제
> 
- 개별 worker node의 disk & input directory 구조
    - Assumption: Each machine has several disks.