name := "sclera-plugin-csv"

description := "Add-on package that enables Sclera to work with CSV format data"

homepage := Some(url(s"https://github.com/scleradb/${name.value}"))

scmInfo := Some(
    ScmInfo(
        url(s"https://github.com/scleradb/${name.value}"),
        s"scm:git@github.com:scleradb/${name.value}.git"
    )
)

versionScheme := Some("early-semver")

version := "4.1-SNAPSHOT"

startYear := Some(2012)

scalaVersion := "2.13.3"

licenses := Seq("Apache License version 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))

resolvers ++= Seq(
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    Resolver.mavenLocal
)

libraryDependencies ++= Seq(
    "org.apache.commons" % "commons-csv" % "1.8",
    "com.scleradb" %% "sclera-io" % "4.1-SNAPSHOT" % "provided",
    "com.scleradb" %% "sclera-core" % "4.1-SNAPSHOT" % "provided",
    "com.scleradb" %% "sclera-config" % "4.1-SNAPSHOT" % "test",
    "org.scalatest" %% "scalatest" % "3.1.1" % "test"
)

scalacOptions ++= Seq(
    "-Werror", "-feature", "-deprecation", "-unchecked"
)

exportJars := true

javaOptions in Test ++= Seq(
    s"-DSCLERA_ROOT=${java.nio.file.Files.createTempDirectory("scleratest")}"
)

fork in Test := true
