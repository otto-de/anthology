package de.otto.capella.config

import org.rogach.scallop.*

final case class CliConf(arguments: Seq[String]) extends ScallopConf(arguments):
    val capellaConfigFile: ScallopOption[String] =
        opt[String](name = AppArgs.CONFIG_FILE_CMD_ARG, noshort = true)
    val capellaAdditionalKafkaProperties: ScallopOption[String] =
        opt[String](name = AppArgs.ADDITIONAL_KAFKA_PROPERTIES_CMD_ARG, noshort = true)
    val capellaStateStorePath: ScallopOption[String] =
        opt[String](name = AppArgs.STATE_STORE_PATH_CMD_ARG, noshort = true)
    verify()
