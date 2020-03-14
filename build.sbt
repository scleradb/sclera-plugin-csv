name := "sclera-plugin-csv"

description := "Add-on package that enables Sclera to work with CSV format data"

version := "4.0-SNAPSHOT"

homepage := Some(url("https://github.com/scleradb/sclera-plugin-csv"))

organization := "com.scleradb"

organizationName := "Sclera, Inc."

organizationHomepage := Some(url("https://www.scleradb.com"))

startYear := Some(2012)

scalaVersion := "2.13.1"

licenses := Seq("Apache License version 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))

libraryDependencies ++= Seq(
    "org.apache.commons" % "commons-csv" % "1.7",
    "com.scleradb" %% "sclera-tools" % "4.0-SNAPSHOT" % "provided",
    "com.scleradb" %% "sclera-core" % "4.0-SNAPSHOT" % "provided",
    "com.scleradb" %% "sclera-config" % "4.0-SNAPSHOT" % "test",
    "org.scalatest" %% "scalatest" % "3.1.0" % "test"
)

scalacOptions ++= Seq(
    "-Werror", "-feature", "-deprecation", "-unchecked"
)

exportJars := true

fork in Test := true
