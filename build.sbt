ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

val PekkoVersion = "1.0.3"
val PekkoHttpVersion = "1.0.1"
val PekkoHttpJsoniterVersion = "2.6.0"
val PekkoKafkaVersion = "1.0.0"
val JsoniterVersion = "2.30.7"
val ScalaTestVersion = "3.2.19"
val TestcontainersVersion = "1.20.1"

lazy val root = (project in file("."))
  .settings(
    name := "exchange-rate-adapter",
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-unchecked",
      "-Xmacro-settings:" + sys.props.getOrElse("macro.settings", "none")
    ),
    compileOrder := CompileOrder.JavaThenScala,
  )
libraryDependencies ++= Seq("org.apache.pekko" %% "pekko-actor-typed" % PekkoVersion,
  "org.apache.pekko" %% "pekko-stream-typed" % PekkoVersion,
  "org.apache.pekko" %% "pekko-http" % PekkoHttpVersion,
  "org.apache.pekko" %% "pekko-connectors-kafka" % PekkoKafkaVersion,
  "com.github.pjfanning" %% "pekko-http-jsoniter-scala" % PekkoHttpJsoniterVersion,
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % JsoniterVersion,
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % JsoniterVersion,
  "ch.qos.logback" % "logback-classic" % "1.3.14"
)