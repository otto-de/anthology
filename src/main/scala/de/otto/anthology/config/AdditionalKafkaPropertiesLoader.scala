package de.otto.anthology.config

import com.fasterxml.jackson.core.`type`.TypeReference
import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.JsonSupport
import de.otto.anthology.kafka.ClusterName

import scala.jdk.CollectionConverters.*

object AdditionalKafkaPropertiesLoader extends LazyLogging:

    private val typeRef: TypeReference[java.util.Map[ClusterName, java.util.Map[String, String]]] =
        new TypeReference {}

    def apply(cliProps: Option[String] = None): AdditionalKafkaPropertiesMap =
        logger.info("Loading additional properties from environment variable ANTHOLOGY_ADDITIONAL_KAFKA_PROPERTIES")
        val json = cliProps.getOrElse(
            sys.env.getOrElse(
                "ANTHOLOGY_ADDITIONAL_KAFKA_PROPERTIES",
                throw new IllegalStateException("ANTHOLOGY_ADDITIONAL_KAFKA_PROPERTIES environment variable is not set")
            )
        )
        fromJson(json)

    def fromJson(json: String): AdditionalKafkaPropertiesMap =
        val rawMap = JsonSupport.mapper.readValue(json, typeRef)
        rawMap.asScala
            .map((k, v) =>
                val props = v.asScala.toMap
                k -> props
            )
            .toMap

type AdditionalKafkaPropertiesMap = Map[ClusterName, Map[String, String]]
