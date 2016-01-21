###                                ESGF WPS API Plugin

_Implements the ESGF-CWT climate data services api for the BDWPS framework._

####  Prerequisite: Install the Scala develpment tools:

    1) Scala:                     http://www.scala-lang.org/download/install.html                   
                        
    
    2) Scala Build Tool (sbt):    http://www.scala-sbt.org/0.13/docs/Setup.html
                        

####  Build the ESGF WPS API plugin:

    1) Checkout the esgfWpsApi sources:

        >> git clone https://github.com/nasa-nccs-cds/esgfWpsApi.git

    2) Build the plugin jar file:

        >> cd esgfWpsApi
        >> sbt package


####  Code development:

    1) Install IntelliJ IDEA CE from https://www.jetbrains.com/idea/download/ with Scala plugin enabled.
    
    2) Start IDEA and import the esgfWpsApi Project from Version Control (github) using the address https://github.com/nasa-nccs-cds/esgfWpsApi.git.
    

####  Project Configuration:

    1) Logging: Edit src/main/resources/logback.xml
    

    

