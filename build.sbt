import sbtassembly.MergeStrategy
import sbtassembly.PathList
import sbtprotoc.ProtocPlugin.autoImport.PB  // ★ ScalaPB 코드 생성용

ThisBuild / scalaVersion := "2.13.16"

val grpcVersion    = "1.62.2"
val scalaPbVersion = "0.11.20"

// 공통 의존성
lazy val sharedDependencies = Seq(
  "io.grpc" % "grpc-netty" % grpcVersion,
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalaPbVersion,
  "org.scalatest" %% "scalatest" % "3.2.19" % Test
)

// ------------------------------------------------------------
// A. common: proto + 코드 생성 (★ 다시 추가된 부분)
// ------------------------------------------------------------
lazy val common = (project in file("common"))
  .settings(
    name := "common",
    libraryDependencies ++= sharedDependencies,
    // ScalaPB 코드 생성
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (Compile / sourceManaged).value
    )
  )

// 공통 merge 전략 (master/worker 둘 다에서 사용)
lazy val commonMergeStrategy: String => MergeStrategy = {
  case PathList("META-INF", "io.netty.versions.properties") =>
    MergeStrategy.first
  case PathList("META-INF", "services", _ @ _*) =>
    MergeStrategy.concat
  case x if x.endsWith(".proto") =>
    MergeStrategy.first
  case PathList("META-INF", _ @ _*) =>
    MergeStrategy.discard
  case _ =>
    MergeStrategy.first
}

// ------------------------------------------------------------
// master 모듈
// ------------------------------------------------------------
lazy val master = (project in file("master"))
  .dependsOn(common)
  .settings(
    name := "master",
    libraryDependencies ++= sharedDependencies,
    Compile / mainClass := Some("master.Master"),
    assembly / assemblyJarName := "master.jar",
    assembly / assemblyMergeStrategy := commonMergeStrategy
  )
  .enablePlugins(sbtassembly.AssemblyPlugin)



// ------------------------------------------------------------
// worker 모듈
// ------------------------------------------------------------
lazy val worker = (project in file("worker"))
  .dependsOn(common)                              // ★ common 의존
  .settings(
    name := "worker",
    libraryDependencies ++= sharedDependencies,
    Compile / mainClass := Some("worker.Worker"), // 네 Worker 오브젝트 FQCN
    assembly / assemblyJarName := "worker.jar",
    assembly / assemblyMergeStrategy := commonMergeStrategy
  )
  .enablePlugins(sbtassembly.AssemblyPlugin)


// ------------------------------------------------------------
// root (aggregate용)
// ------------------------------------------------------------
lazy val root = (project in file("."))
  .aggregate(common, master, worker)
  .settings(
    publish / skip := true
  )