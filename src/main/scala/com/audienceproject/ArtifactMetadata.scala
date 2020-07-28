package com.audienceproject

case class ArtifactMetadata(uuid: String, artifactType: String, timestamp: Long, location: Option[String], metadata: collection.Map[String, Any])
