package de.otto.anthology.config

import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.DomainName
import de.otto.anthology.Parallelism
import de.otto.anthology.statestore.RocksDBConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader
import pureconfig.module.yaml.*

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Paths
import scala.reflect.ClassTag

object AnthologyConfigFactory extends LazyLogging:

    private def resourcePath(path: String): Path =
        Paths.get(URLDecoder.decode(getClass.getResource("/" + path).getFile, StandardCharsets.UTF_8))

    def apply(): AnthologyConfig =
        logger.info("Loading Anthology configuration")
        val path: Path =
            sys.env
                .get("ANTHOLOGY_CONFIG_FILE")
                .map(Paths.get(_))
                .getOrElse(resourcePath("application.yaml"))
        logger.info(s"loading config from $path")
        YamlConfigSource.file(path).loadOrThrow[AnthologyConfig]

// 'derives ConfigReader' doesn't work yet for default values
// see https://github.com/pureconfig/pureconfig/issues/1488
// see https://github.com/pureconfig/pureconfig/issues/1673

case class AnthologyConfig(
    name: String,
    domains: Seq[DomainConfig],
    domainRelations: Seq[DomainRelationConfig],
    codomain: CodomainConfig,
    kafkaClusters: Seq[KafkaClusterConfig],
    rocksDB: RocksDBConfig = RocksDBConfig(),
    parallelism: Parallelism = Parallelism(1)
):
    val domainsByName: Map[DomainName, DomainConfig] = domains.map(d => (d.name, d)).toMap

object AnthologyConfig:
    given ConfigReader[AnthologyConfig] = deriveReader[AnthologyConfig]
