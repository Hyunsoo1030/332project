resolvers ++= Resolver.sonatypeOssRepos("snapshots")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.3")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.3.1")

val zioGrpcVersion = "0.6.0-rc5"

libraryDependencies ++= Seq(
  "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % zioGrpcVersion,
  "com.thesamet.scalapb" %% "compilerplugin" % "0.11.10"
)