
import sbt._

object Version {
  val logback   = "1.1.2"
  val mockito   = "1.10.19"
  val scala     = "2.11.7"
  val scalaTest = "2.2.4"
  val slf4j     = "1.7.6"
}

object Library {
  val logbackClassic = "ch.qos.logback"    %  "logback-classic" % Version.logback
  val mockitoAll     = "org.mockito"       %  "mockito-all"     % Version.mockito
  val scalaTest      = "org.scalatest"     %% "scalatest"       % Version.scalaTest
  val slf4jApi       = "org.slf4j"         %  "slf4j-api"       % Version.slf4j
  val scalaxml       = "org.scala-lang.modules" %% "scala-xml" % "1.0.3"
  val scalaparser    = "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3"
  val breezeMath     = "org.scalanlp"      %% "breeze-math_2.10" %  "0.4"
  val nd4s           = "org.nd4j"           % "nd4s_2.11"       % "0.4-rc3.8"
  val nd4j           =  "org.nd4j"          % "nd4j-x86"        % "0.4-rc3.8"
  val joda           = "joda-time"          % "joda-time"       % "2.8.1"
  val natty          = "com.joestelmach"    % "natty"           % "0.11"
  val geotools       = "org.geotools"      %  "gt-shapefile"    % "13.2"
  val netcdfAll      = "edu.ucar"           % "netcdf"          % "4.3.23"
  val scalactic      = "org.scalactic" %% "scalactic" % "2.2.6"
  val scalatest      = "org.scalatest" %% "scalatest" % "2.2.6" % "test"
}

object Dependencies {
  import Library._

  val scala = Seq( logbackClassic, slf4jApi, scalaxml, scalaparser, joda, natty, scalactic, scalatest  )

  val ndarray = Seq( nd4s, nd4j )

  val netcdf = Seq( netcdfAll )

  val geo  = Seq( geotools )
}










