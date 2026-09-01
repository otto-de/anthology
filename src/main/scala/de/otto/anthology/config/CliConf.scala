package de.otto.anthology.config

import org.rogach.scallop.*

final case class CliConf(arguments: Seq[String]) extends ScallopConf(arguments):
    val anthologyConfigFile: ScallopOption[String] =
        opt[String](name = AppArgs.CONFIG_FILE_CMD_ARG, noshort = true)
    val anthologyAdditionalKafkaProperties: ScallopOption[String] =
        opt[String](name = AppArgs.ADDITIONAL_KAFKA_PROPERTIES_CMD_ARG, noshort = true)
    val anthologyStateStorePath: ScallopOption[String] =
        opt[String](name = AppArgs.STATE_STORE_PATH_CMD_ARG, noshort = true)
    verify()
