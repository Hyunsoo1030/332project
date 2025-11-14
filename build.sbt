import sbtprotoc.ProtocPlugin.autoImport._
import scalapb.GeneratorOption

ThisBuild / version       := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion  := "3.3.7"

// --- gRPC and Protobuf Configuration ---
// Protobuf 파일의 기본 위치 (예: common/src/main/protobuf)
val protoDir = "src/main/protobuf"

// 모든 모듈이 접근해야 하는 gRPC/Protobuf 핵심 라이브러리
val grpcDependencies = Seq(
  // ScalaPB 런타임 (자동 생성된 메시지 클래스 지원)
  "com.thesamet.scalapb" %% "scalapb-runtime" % "0.11.14",
  // ScalaPB gRPC 런타임 (자동 생성된 서비스 인터페이스/Stub 지원)
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % "0.11.14",

  // gRPC Netty 구현 (실제 네트워크 통신 엔진)
  "io.grpc" % "grpc-netty" % "1.60.0",

  // gRPC 기타 유틸리티 라이브러리
  "io.grpc" % "grpc-stub" % "1.60.0",
  "io.grpc" % "grpc-protobuf" % "1.60.0",
)
// ----------------------------------------


// 공통 모듈: Protobuf 정의 및 라이브러리 포함
lazy val common = (project in file("common"))
  .settings(
    name := "common",
    // 모든 gRPC 관련 의존성을 common에 추가
    libraryDependencies ++= grpcDependencies,

    // ScalaPB가 생성한 코드를 컴파일 경로에 포함시킵니다.
    Compile / managedSourceDirectories += (Compile / scalaSource).value / "scalapb"
  )

// worker 모듈: common에 의존하며 gRPC 클라이언트 역할
lazy val worker = (project in file("worker"))
  .dependsOn(common)
  .settings(
    name := "worker"
    // run / mainClass := Some("com.project332.worker.WorkerClient") // 실제 main 클래스 설정 (선택)
  )

// master 모듈: common에 의존하며 gRPC 서버 역할
lazy val master = (project in file("master"))
  .dependsOn(common)
  .settings(
    name := "master"
    // run / mainClass := Some("com.project332.master.MasterServer") // 실제 main 클래스 설정 (선택)
  )

// 루트(aggregator) 프로젝트
lazy val root = (project in file("."))
  .aggregate(common, worker, master)
  .settings(
    name := "332project",
    publish / skip := true
  )