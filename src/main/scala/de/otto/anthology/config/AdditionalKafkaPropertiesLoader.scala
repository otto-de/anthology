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
        val json = cliProps.getOrElse(
            sys.env.getOrElse(
                AppArgs.ADDITIONAL_KAFKA_PROPERTIES_ENV_VAR,
                throw new IllegalStateException(
                    s"${AppArgs.ADDITIONAL_KAFKA_PROPERTIES_ENV_VAR} environment variable is not set and " +
                        s"no command-line argument ${AppArgs.ADDITIONAL_KAFKA_PROPERTIES_CMD_ARG} given"
                )
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
