package com.audienceproject

import java.math.BigInteger
import java.util.UUID

import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item, PrimaryKey, Table}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import com.audienceproject.types.Json
import org.joda.time.LocalDate

import scala.collection.JavaConverters._

class ArtifactsMetadataClient(dynamoDbClient: AmazonDynamoDB = AmazonDynamoDBClientBuilder.defaultClient(), tableName: String = "artifacts_metadata") {
    private val table: Table = new DynamoDB(dynamoDbClient).getTable(tableName)
    private val uuidKey: String = "id"
    private val artifactTypeKey: String = "artifactType"
    private val timestampKey: String = "timestamp"
    private val locationKey: String = "location"
    private val dayKey: String = "dd"
    private val monthKey: String = "mm"
    private val yearKey: String = "yyyy"
    private val locationUidPlaceholder: String = "{artifact-id}"

    /**
     * Adds an artifact metadata entry
     *
     * @param artifactType The type of artifact in string format
     * @param timestamp    The unixtimestamp creation time. Is automatically set if not overwritten
     * @param uuid         The uuid that identifies this artifact. Is automatically set if not overwritten
     * @param metadata     The collection of column names and values that is to be stored for this artifact.
     * @param location     The location of the artifact. If it contains {artifact-id}, it will be replaced with the generated uuid.
     * @return Uuid for the artifact
     */
    def log(artifactType: String, timestamp: Long = System.currentTimeMillis(), uuid: String = getUuid, location: Option[String] = None, metadata: Map[String, Any] = Map.empty[String, String]): String = {
        val locationWithUid = location match {
            case Some(s) => Some(s.replace(locationUidPlaceholder, uuid))
            case None => None
        }
        table.putItem(toItem(uuid, artifactType, timestamp, metadata, locationWithUid))
        uuid
    }

    /**
     * Sugar method to easily add reference time in correct format.
     * This makes it easy for other projects to resolve dependencies, as they can query by type and reference time
     *
     * @param artifactType The type of artifact in string format
     * @param timestamp    The unixtimestamp creation time. Is automatically set if not overwritten
     * @param uuid         The uuid that identifies this artifact. Is automatically set if not overwritten
     * @param metadata     The collection of column names and values that is to be stored for this artifact.
     * @param dd           Reference date of the artifact. This is to easily resolve dependencies for other projects
     * @param mm           Reference date of the artifact. This is to easily resolve dependencies for other projects
     * @param yyyy         Reference date of the artifact. This is to easily resolve dependencies for other projects
     * @param location     The location of the artifact
     * @return Uuid for the artifact
     */
    def logWithReferenceTime(artifactType: String, yyyy: Int, mm: Int, dd: Int, timestamp: Long = System.currentTimeMillis(), uuid: String = getUuid, location: Option[String] = None, metadata: Map[String, Any] = Map.empty[String, String]): String = {
        log(artifactType, timestamp, uuid, location, metadata ++ Map(dayKey -> dd, monthKey -> mm, yearKey -> yyyy))
    }

    def logWithReferenceTimeAsDateString(artifactType: String, date: String, timestamp: Long = System.currentTimeMillis(), uuid: String = getUuid, location: Option[String] = None, metadata: Map[String, Any] = Map.empty[String, String]): String = {
        val dateAsInts = toIntTuple(date)
        logWithReferenceTime(artifactType, dateAsInts._1, dateAsInts._2, dateAsInts._3, timestamp, uuid, location, metadata)
    }

    def getLatestBefore(artifactType: String, yyyy: Int, mm: Int, dd: Int, latest: Boolean = true, metadata: Map[String, Any] = Map.empty[String, String]): Option[ArtifactMetadata] = {
        val latestBeforeExpression: String = s"$yearKey <= :$yearKey and ($yearKey <> :$yearKey or $monthKey <= :$monthKey) and (($yearKey <> :$yearKey or $monthKey <> :$monthKey) or $dayKey <= :$dayKey)"
        val attributes = metadata.toSeq :+ (artifactTypeKey, artifactType)

        val placeholderNames: Map[String, String] = getExpressionKeyPlaceholderNames(attributes.map({ case (k, _) => k }))
        val filterExpression = Seq(toFilterExpression(placeholderNames.toSeq), latestBeforeExpression).mkString(" and ")
        val valueMap = toValueMap(attributes).withInt(s":$yearKey", yyyy).withInt(s":$monthKey", mm).withInt(s":$dayKey", dd)

        table.scan(filterExpression, placeholderNames.asJava, valueMap)
          .asScala
          .map(item => toScalaObject(item))
          .toSeq
          .sortBy({
              if (latest) {
                  -_.timestamp
              } else {
                  _.timestamp
              }
          })
          .sortBy(artifact => artifact.metadata.get(dayKey) match {
              case Some(i: java.math.BigDecimal) => -i.longValue
              case _ => Long.MaxValue
          })
          .sortBy(artifact => artifact.metadata.get(monthKey) match {
              case Some(i: java.math.BigDecimal) => -i.longValue
              case _ => Long.MaxValue
          })
          .sortBy(artifact => artifact.metadata.get(yearKey) match {
              case Some(i: java.math.BigDecimal) => -i.longValue
              case _ => Long.MaxValue
          })
          .headOption
    }

    def getLatestBeforeWithDateString(artifactType: String, date: String, latest: Boolean = true, metadata: Map[String, Any] = Map.empty[String, String]): Option[ArtifactMetadata] = {
        val dateAsInts = toIntTuple(date)
        getLatestBefore(artifactType, dateAsInts._1, dateAsInts._2, dateAsInts._3, latest, metadata)
    }

    private def toIntTuple(date: String): (Int, Int, Int) = {
        val parsedDate = LocalDate.parse(date)
        val yyyy: Int = parsedDate.getYear
        val mm: Int = parsedDate.getMonthOfYear
        val dd: Int = parsedDate.getDayOfMonth
        (yyyy, mm, dd)
    }

    /**
     * Returns an ArtifactMetaData matching the search criteria by doing a DynamoDB scan
     *
     * @param artifactType the string that identifies the artifact type
     * @param latest       if true, the artifact that was logged latest is returned. If false, the artifact that was logged first is returned
     * @param metadata     specifies the search based on exact match, where a key in the map is a column name and the value is the exact value
     * @return A single artifact, with multiple matches being resolved by time of logging
     */
    def getByType(artifactType: String, latest: Boolean = true, metadata: Map[String, Any] = Map.empty[String, String]): Option[ArtifactMetadata] = {
        val attributes = metadata.toSeq :+ (artifactTypeKey, artifactType)
        val placeholderNames: Map[String, String] = getExpressionKeyPlaceholderNames(attributes.map({ case (k, _) => k }))

        table.scan(toFilterExpression(placeholderNames.toSeq), placeholderNames.asJava, toValueMap(attributes))
          .asScala
          .map(item => toScalaObject(item))
          .toSeq
          .sortBy({
              if (latest) {
                  -_.timestamp
              } else {
                  _.timestamp
              }
          })
          .headOption
    }

    def getExpressionKeyPlaceholderNames(attributesNames: Seq[String]): Map[String, String] = {
        val increasing = 0 to attributesNames.size
        attributesNames
          .zip(increasing)
          .map({ case (keyName, i) => (keyName, s"#$i") })
          .foldLeft(Map.empty[String, String])({ case (map: Map[String, String], (name: String, placeholder: String)) => map + (placeholder -> name) })
    }

    private def toFilterExpression(placeholderNames: Seq[(String, String)]): String = {
        placeholderNames
          .map({ case (k, v) => s"$k = :$v" })
          .mkString(" and ")
    }

    private def toValueMap(attributes: Seq[(String, Any)]): ValueMap = {
        attributes
          .foldLeft(new ValueMap())({ case (valueMap, (key, value)) => put(s":$key", value, valueMap) })
    }

    def getByUuid(uuid: String): Option[ArtifactMetadata] = {
        val spec = new QuerySpec()
          .withKeyConditionExpression("id = :id")
          .withValueMap(new ValueMap()
            .withString(":id", uuid))
        table.query(spec)
          .asScala
          .headOption
          .map(toScalaObject)
    }

    private def toScalaObject(item: Item): ArtifactMetadata = {
        val metadata = toMetaData(item)
        val location: Option[String] = metadata.get(locationKey) match {
            case Some(str: String) => Some(str)
            case _ => None
        }
        ArtifactMetadata(
            item.getString(uuidKey),
            item.getString(artifactTypeKey),
            item.getLong(timestampKey),
            location,
            metadata
        )
    }

    private def toMetaData(item: Item): collection.Map[String, Any] = {
        item
          .asMap
          .asScala
          .filterKeys(k => !Set(uuidKey, artifactTypeKey, timestampKey).contains(k))
          .map({ case (k, v) => (k.asInstanceOf[String], v) })
    }

    private def toItem(uuid: String, artifactType: String, timestamp: Long, metadata: Map[String, Any], location: Option[String] = None): Item = {
        val metadataWithLocation = location match {
            case Some(str) => metadata + (locationKey -> str)
            case None => metadata
        }

        val itemWithMandatoryValues = new Item()
          .withPrimaryKey(uuidKey, uuid)
          .withString(artifactTypeKey, artifactType)
          .withBigInteger(timestampKey, BigInteger.valueOf(timestamp))

        metadataWithLocation.toSeq
          .sortBy({ case (key, _) => key })
          .foldLeft(itemWithMandatoryValues)({ case (item, (key, value)) => put(key, value, item) })
    }

    def getUuid: String = UUID.randomUUID().toString

    def put[T](key: String, value: Any, valueMap: ValueMap): ValueMap = {
        value match {
            case v: Number => valueMap.withNumber(key, v)
            case v: String => valueMap.withString(key, v)
            case v: Boolean => valueMap.withBoolean(key, v)
            case v: Map[String, Any] if v.nonEmpty && v.head._1.isInstanceOf[String] => valueMap.withMap(key, v.asJava)
            case v: Seq[_] => valueMap.withList(key, v.toList.asJava)
            case v: List[_] => valueMap.withList(key, v.asJava)
            case v: Set[String] if v.nonEmpty && v.head.isInstanceOf[String] => valueMap.withStringSet(key, v.asJava)
            case v: Set[Number] if v.nonEmpty && v.head.isInstanceOf[Number] => valueMap.withNumberSet(key, v.map(n => java.math.BigDecimal.valueOf(n.doubleValue())).asJava)
            case v: Json => valueMap.withJSON(key, v.body)
            case _ => throw new IllegalArgumentException(s"Type of column with key $key does not match any implemented data type. The type provided is ${value.getClass}.")
        }
    }

    def put[T](key: String, value: Any, item: Item): Item = {
        value match {
            case v: Number => item.withNumber(key, v)
            case v: String => item.withString(key, v)
            case v: Boolean => item.withBoolean(key, v)
            case v: Map[String, Any] if v.nonEmpty && v.head._1.isInstanceOf[String] => item.withMap(key, v.asJava)
            case v: Seq[_] => item.withList(key, v.toList.asJava)
            case v: List[_] => item.withList(key, v.asJava)
            case v: Set[String] if v.nonEmpty && v.head.isInstanceOf[String] => item.withStringSet(key, v.asJava)
            case v: Set[Number] if v.nonEmpty && v.head.isInstanceOf[Number] => item.withNumberSet(key, v.asJava)
            case v: Json => item.withJSON(key, v.body)
            case _ => throw new IllegalArgumentException(s"Type of column with key $key does not match any implemented data type. The type provided is ${value.getClass}.")
        }
    }

    def delete(uuid: String): Unit = {
        table.deleteItem(new PrimaryKey().addComponent(uuidKey, uuid))
    }
}