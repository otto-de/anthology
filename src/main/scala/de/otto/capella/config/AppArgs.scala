package de.otto.capella.config

object AppArgs:

    val CONFIG_FILE_ENV_VAR: String = "CAPELLA_CONFIG_FILE"
    val CONFIG_FILE_CMD_ARG: String = "capella-config-file"

    val ADDITIONAL_KAFKA_PROPERTIES_ENV_VAR: String = "CAPELLA_ADDITIONAL_KAFKA_PROPERTIES"
    val ADDITIONAL_KAFKA_PROPERTIES_CMD_ARG: String = "capella-additional-kafka-properties"

    val STATE_STORE_PATH_ENV_VAR: String = "CAPELLA_STATE_STORE_PATH"
    val STATE_STORE_PATH_CMD_ARG: String = "capella-state-store-path"
