// Run `sbt dependencyUpdates` if you want to see what dependencies can be updated
// Run `sbt dependencyGraph` if you want to see the dependencies

/**
  * The groupId in Maven
  */
organization := "com.audienceproject"

/**
  * The artefactId in Maven
  */
name := "artifacts-metadata"

/**
  * The version must match "&#94;(\\d+\\.\\d+\\.\\d+)$" to be considered a release
  */
version := "1.0.0"
description := "A simple library for managing artifacts with complex metadata."

scalaVersion := "2.12.12"

/**
  * Additional scala version supported.
  */
crossScalaVersions := Seq("2.11.12", "2.12.12")

compileOrder := CompileOrder.JavaThenScala

/**
 * Dependencies for the whole project
 */

libraryDependencies += "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.336"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test

/**
 * Maven specific settings for publishing to Maven central.
 */
publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }
publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
    else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
pomExtra := <url>https://github.com/audienceproject/artifacts-metadata-jvm</url>
  <licenses>
      <license>
          <name>Apache License, Version 2.0</name>
          <url>https://opensource.org/licenses/apache-2.0</url>
      </license>
  </licenses>
  <scm>
      <url>git@github.com:audienceproject/artifacts-metadata-jvm.git</url>
      <connection>scm:git:git//github.com/audienceproject/artifacts-metadata-jvm.git</connection>
      <developerConnection>scm:git:ssh://github.com:audienceproject/artifacts-metadata-jvm.git</developerConnection>
  </scm>
  <developers>
      <developer>
          <id>cosmincatalin</id>
          <name>Cosmin Catalin Sanda</name>
          <email>cosmin@audienceproject.com</email>
          <organization>AudienceProject</organization>
          <organizationUrl>https://www.audienceproject.com</organizationUrl>
      </developer>
  </developers>