name := """CDAPI"""

organization := "nasa.nccs"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.7"

lazy val root = project in file(".")

// ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

// resolvers ++= Seq( "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases" )

resolvers += "Unidata maven repository" at "http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases"

libraryDependencies ++= Dependencies.scala

libraryDependencies ++= Dependencies.ndarray

// libraryDependencies ++= Dependencies.netcdf    Maven version of Jar does not work- Currently unmanaged.