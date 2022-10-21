import BuildHelper._

Global / onChangedBuildSource := ReloadOnSourceChanges

inThisBuild(
  List(
    developers := List(
      Developer("jdegoes", "John De Goes", "john@degoes.net", url("https://degoes.net")),
      Developer("mijicd", "Dejan Mijic", "dmijic@acm.org", url("https://github.com/mijicd"))
    ),
    homepage         := Some(url("https://github.com/zio/zio-memcached/")),
    licenses         := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    organization     := "dev.zio",
    organizationName := "John A. De Goes and the ZIO contributors",
    startYear        := Some(2021)
  )
)

addCommandAlias("compileBenchmarks", "benchmarks/Jmh/compile")
addCommandAlias("compileSources", "example/Test/compile; memcached/Test/compile")
addCommandAlias("check", "fixCheck; fmtCheck")
addCommandAlias("fix", "scalafixAll")
addCommandAlias("fixCheck", "scalafixAll --check")
addCommandAlias("fmt", "all scalafmtSbt scalafmtAll")
addCommandAlias("fmtCheck", "all scalafmtSbtCheck scalafmtCheckAll")
addCommandAlias("prepare", "fix; fmt")

lazy val root =
  project
    .in(file("."))
    .settings(publish / skip := true)
    // .aggregate(memcached, benchmarks, example)
    .aggregate(memcached, example)

lazy val memcached =
  project
    .in(file("memcached"))
    .enablePlugins(BuildInfoPlugin)
    .settings(buildInfoSettings("zio.memcached"))
    .settings(scala3Settings)
    .settings(stdSettings("zio-memcached"))
    .settings(
      libraryDependencies ++= List(
        "dev.zio"                %% "zio-streams"             % "2.0.2",
        "dev.zio"                %% "zio-logging"             % "2.1.2",
        "dev.zio"                %% "zio-schema"              % "0.2.1",
        "dev.zio"                %% "zio-schema-protobuf"     % "0.2.1" % Test,
        "dev.zio"                %% "zio-test"                % "2.0.2" % Test,
        "dev.zio"                %% "zio-test-sbt"            % "2.0.2" % Test,
        "org.scala-lang.modules" %% "scala-collection-compat" % "2.8.1"
      ),
      testFrameworks := List(new TestFramework("zio.test.sbt.ZTestFramework"))
    )
/*
lazy val benchmarks =
  project
    .in(file("benchmarks"))
    .enablePlugins(JmhPlugin)
    .dependsOn(memcached)
    .settings(stdSettings("benchmarks"))
    .settings(
      crossScalaVersions -= Scala3,
      publish / skip := true,
      libraryDependencies ++= List(
        "dev.profunktor"    %% "redis4cats-effects"  % "1.2.0",
        "io.chrisdavenport" %% "rediculous"          % "0.4.0",
        "io.laserdisc"      %% "laserdisc-fs2"       % "0.5.0",
        "dev.zio"           %% "zio-schema-protobuf" % "0.2.1"
      )
    )
 */
lazy val example =
  project
    .in(file("example"))
    .dependsOn(memcached)
    .settings(stdSettings("example"))
    .settings(
      publish / skip := true,
      libraryDependencies ++= List(
        "dev.zio"  %% "zio-streams"         % "2.0.2",
        "dev.zio"  %% "zio-config-magnolia" % "3.0.2",
        "dev.zio"  %% "zio-config-typesafe" % "3.0.2",
        "dev.zio"  %% "zio-schema-protobuf" % "0.2.1",
        "dev.zio"  %% "zio-json"            % "0.3.0-RC11",
        "io.d11"   %% "zhttp"               % "2.0.0-RC11",
        "org.slf4j" % "slf4j-simple"        % "2.0.3"
      )
    )

lazy val docs = project
  .in(file("zio-memcached-docs"))
  .enablePlugins(MdocPlugin, DocusaurusPlugin, ScalaUnidocPlugin)
  .dependsOn(memcached)
  .settings(
    publish / skip := true,
    moduleName     := "zio-memcached-docs",
    scalacOptions -= "-Yno-imports",
    scalacOptions -= "-Xfatal-warnings",
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(memcached),
    ScalaUnidoc / unidoc / target              := (LocalRootProject / baseDirectory).value / "website" / "static" / "api",
    cleanFiles += (ScalaUnidoc / unidoc / target).value,
    docusaurusCreateSite     := docusaurusCreateSite.dependsOn(Compile / unidoc).value,
    docusaurusPublishGhpages := docusaurusPublishGhpages.dependsOn(Compile / unidoc).value
  )
  .settings(macroDefinitionSettings)
