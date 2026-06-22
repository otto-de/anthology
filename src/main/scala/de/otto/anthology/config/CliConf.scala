package de.otto.anthology.config

import org.rogach.scallop.*

final case class CliConf(arguments: Seq[String]) extends ScallopConf(arguments):
    val anthologyConfigFile: ScallopOption[String] = opt[String](name = "anthology-config-file", noshort = true)
    val anthologyCredentials: ScallopOption[String] = opt[String](name = "anthology-credentials", noshort = true)
    val anthologyStateStorePath: ScallopOption[String] =
        opt[String](name = "anthology-state-store-path", noshort = true)
    verify()
