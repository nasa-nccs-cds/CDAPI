name := """CDAPI"""

organization := "nasa.nccs"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.7"

lazy val root = project in file(".")

// ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

// resolvers ++= Seq( "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases" )

resolvers += "Unidata maven repository" at "http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases"
resolvers += "Java.net repository" at "http://download.java.net/maven/2"
resolvers += "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools"
resolvers += "Boundless Maven Repository" at "http://repo.boundlessgeo.com/main"
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

libraryDependencies ++= Dependencies.scala

libraryDependencies ++= Dependencies.geo

// libraryDependencies ++= Dependencies.netcdf