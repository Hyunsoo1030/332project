ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.7"

// 공통 모듈
lazy val common = (project in file("common"))
  .settings(
    name := "common"
  )

// worker 모듈
lazy val worker = (project in file("worker"))
  .dependsOn(common)
  .settings(
    name := "worker"
  )

// master 모듈
lazy val master = (project in file("master"))
  .dependsOn(common)
  .settings(
    name := "master"
  )

// 루트(aggregator) 프로젝트
lazy val root = (project in file("."))
  .aggregate(common, worker, master)
  .settings(
    name := "332project",
    publish / skip := true
  )
