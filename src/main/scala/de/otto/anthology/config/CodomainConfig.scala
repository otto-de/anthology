package de.otto.anthology.config

import de.otto.anthology.CodomainDeduplicationConfig
import de.otto.anthology.KafkaSinkConfig
import de.otto.anthology.filtering.CodomainFilteringConfig
import de.otto.anthology.headerpropagation.HeaderPropagationConfig
import de.otto.anthology.headerpropagation.HeaderPropagationConfigs
import de.otto.anthology.transformation.CodomainTransformationConfig
import pureconfig.ConfigReader

case class CodomainConfig(
    deduplication: Option[CodomainDeduplicationConfig],
    filtering: Option[CodomainFilteringConfig],
    transformation: Option[CodomainTransformationConfig],
    headerPropagation: Option[Seq[HeaderPropagationConfig]],
    kafka: KafkaSinkConfig,
    logSentMessages: Option[Boolean],
    logThroughput: Option[Boolean]
) derives ConfigReader:
    def headerPropagationConfigs: Option[HeaderPropagationConfigs] =
        headerPropagation.fold(None): hp =>
            if hp.isEmpty then None else Some(HeaderPropagationConfigs(hp))
