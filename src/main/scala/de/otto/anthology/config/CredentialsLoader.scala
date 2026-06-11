package de.otto.anthology.config

import com.fasterxml.jackson.core.`type`.TypeReference
import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.JsonSupport
import de.otto.anthology.kafka.ClusterName

import scala.jdk.CollectionConverters.*

object CredentialsLoader extends LazyLogging:

    private val typeRef: TypeReference[java.util.Map[ClusterName, java.util.Map[String, String]]] =
        new TypeReference {}

    def apply(): CredentialsMap =
        logger.info("Loading credentials from environment variable ANTHOLOGY_CREDENTIALS")
        val json = sys.env.getOrElse(
            "ANTHOLOGY_CREDENTIALS",
            throw new IllegalStateException("ANTHOLOGY_CREDENTIALS environment variable is not set")
        )
        fromJson(json)

    def fromJson(json: String): CredentialsMap =
        val rawMap = JsonSupport.mapper.readValue(json, typeRef)
        rawMap.asScala
            .map((k, v) =>
                val credentials = v.asScala.toMap
                require(
                    credentials.contains("username") && credentials.contains("password"),
                    s"Credentials for domain '$k' must contain both 'username' and 'password' keys"
                )
                k -> credentials
            )
            .toMap

type CredentialsMap = Map[ClusterName, Map[String, String]]
