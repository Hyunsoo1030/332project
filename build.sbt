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

    assembly / mainClass := Some("worker.Worker"),      // 실제 워커 main object 이름
    assembly / assemblyJarName := "worker-assembly.jar",

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "io.netty.versions.properties") =>
        MergeStrategy.first
      case PathList("META-INF", xs @ _*) =>
        MergeStrategy.discard
      case _ =>
        MergeStrategy.first
    }
  )

// D. root: aggregator
lazy val root = project.in(file("."))
  .aggregate(common, master, worker)
  .settings(
    publish := {}
  )
