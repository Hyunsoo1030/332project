import sbtassembly.MergeStrategy
import sbtassembly.PathList

scalaVersion := "2.13.16"

val grpcVersion    = "1.62.2"
val scalaPbVersion = "0.11.20"

// 공통 의존성
lazy val sharedDependencies = Seq(
  "io.grpc" % "grpc-netty" % grpcVersion,
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalaPbVersion
)

lazy val commonAssemblySettings = Seq(
  assembly / assemblyMergeStrategy := {
    // 1) netty 버전 파일은 첫 번째 것만 사용
    case PathList("META-INF", "io.netty.versions.properties") =>
      MergeStrategy.first

    // 2) ServiceLoader용 파일은 여러 JAR의 내용을 이어 붙여야 함
    //    gRPC가 여기서 ManagedChannelProvider, NameResolverProvider 등을 찾음
    case PathList("META-INF", "services", xs @ _*) =>
      MergeStrategy.concat

    // 3) 그 외 META-INF 잡다한 것들은 버려도 됨 (라이선스, 서명 등)
    case PathList("META-INF", xs @ _*) =>
      MergeStrategy.discard

    // 4) 나머지는 기본적으로 첫 번째 것을 사용
    case _ =>
      MergeStrategy.first
  }
)

// A. common: proto + 코드 생성
lazy val common = project.in(file("common"))
  .settings(
    scalaVersion := "2.13.16",
    libraryDependencies ++= sharedDependencies,
    // ScalaPB 코드 생성
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (Compile / sourceManaged).value
    )
  )

// B. master: gRPC 서버
lazy val master = project.in(file("master"))
  .dependsOn(common)
  .settings(
    scalaVersion := "2.13.16",
    libraryDependencies ++= sharedDependencies,
    Compile / unmanagedSourceDirectories +=
      (common / Compile / sourceManaged).value // (옵션) common 생성 소스를 직접 참조하고 싶다면
  )

// C. worker: gRPC 클라이언트
lazy val worker = project.in(file("worker"))
  .dependsOn(common)
  .settings(
    scalaVersion := "2.13.16",
    libraryDependencies ++= sharedDependencies,
    Compile / unmanagedSourceDirectories +=
      (common / Compile / sourceManaged).value,
    assembly / mainClass := Some("worker.Worker"),      // 실제 main object
    assembly / assemblyJarName := "worker-assembly.jar"
  )
  .settings(commonAssemblySettings: _*)

// D. root: aggregator
lazy val root = project.in(file("."))
  .aggregate(common, master, worker)
  .settings(
    publish := {}
  )
