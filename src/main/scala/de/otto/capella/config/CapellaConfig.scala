package de.otto.capella.config

import com.typesafe.scalalogging.LazyLogging
import de.otto.capella.statestore.RocksDBConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader
import pureconfig.module.yaml.*

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Paths
import scala.reflect.ClassTag

object CapellaConfigFactory extends LazyLogging:

    private def resourcePath(path: String): Path =
        Paths.get(URLDecoder.decode(getClass.getResource("/" + path).getFile, StandardCharsets.UTF_8))

    def apply(cliPath: Option[String] = None): CapellaConfig =
        logger.info("Loading Capella configuration")
        val path: Path =
            cliPath
                .orElse(sys.env.get(AppArgs.CONFIG_FILE_ENV_VAR))
                .map(Paths.get(_))
                .getOrElse(resourcePath("application.yaml"))
        logger.info(s"loading config from $path")
        YamlConfigSource.file(path).loadOrThrow[CapellaConfig]

// 'derives ConfigReader' doesn't work yet for default values
// see https://github.com/pureconfig/pureconfig/issues/1488
// see https://github.com/pureconfig/pureconfig/issues/1673

case class CapellaConfig(
    name: String,
    domain: DomainConfig,
    codomain: CodomainConfig,
    kafkaClusters: Seq[KafkaClusterConfig],
    rocksDB: RocksDBConfig = RocksDBConfig()
)

object CapellaConfig:
    given ConfigReader[CapellaConfig] = deriveReader[CapellaConfig]
