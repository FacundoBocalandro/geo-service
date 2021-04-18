name := "geo-service"

version := "0.1"

scalaVersion := "2.13.5"

val circeVersion = "0.12.3"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

libraryDependencies ++= Seq(
  "io.grpc" % "grpc-netty" % "1.4.0",
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
  "io.etcd" % "jetcd-core" % "0.5.4",
  "com.github.coreos" % "jetcd" % "0.0.2",
  "com.github.cb372" %% "scalacache-memcached" % "0.28.0"
)

Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
)


libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
)

libraryDependencies += "com.thesamet.scalapb" %% "scalapb-json4s" % "0.11.0"

resolvers ++= Seq(
  "jitpack" at "https://jitpack.io/"
)
