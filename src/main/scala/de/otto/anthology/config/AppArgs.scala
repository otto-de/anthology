package de.otto.anthology.config

object AppArgs:

    val CONFIG_FILE_ENV_VAR: String = "ANTHOLOGY_CONFIG_FILE"
    val CONFIG_FILE_CMD_ARG: String = "anthology-config-file"

    val ADDITIONAL_KAFKA_PROPERTIES_ENV_VAR: String = "ANTHOLOGY_ADDITIONAL_KAFKA_PROPERTIES"
    val ADDITIONAL_KAFKA_PROPERTIES_CMD_ARG: String = "anthology-additional-kafka-properties"

    val STATE_STORE_PATH_ENV_VAR: String = "ANTHOLOGY_STATE_STORE_PATH"
    val STATE_STORE_PATH_CMD_ARG: String = "anthology-state-store-path"
