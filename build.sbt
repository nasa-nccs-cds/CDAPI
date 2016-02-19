name := """CDAPI"""

organization := "nasa.nccs"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.7"

lazy val root = project in file(".")

// ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

// resolvers ++= Seq( "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases" )

libraryDependencies ++= Dependencies.scala

libraryDependencies ++= Dependencies.ndarray