package de.otto.anthology.config

import org.rogach.scallop.*

final case class CliConf(arguments: Seq[String]) extends ScallopConf(arguments):
    val anthologyConfigFile: ScallopOption[String] = opt[String](name = "anthology-config-file", noshort = true)
    val anthologyAdditionalKafkaProperties: ScallopOption[String] =
        opt[String](name = "anthology-additional-kafka-properties", noshort = true)
    val anthologyStateStorePath: ScallopOption[String] =
        opt[String](name = "anthology-state-store-path", noshort = true)
    verify()
