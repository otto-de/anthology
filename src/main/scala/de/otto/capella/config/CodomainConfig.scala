package de.otto.capella.config

import de.otto.capella.CodomainDeduplicationConfig
import de.otto.capella.KafkaSinkConfig
import de.otto.capella.filtering.CodomainFilteringConfig
import de.otto.capella.headerpropagation.HeaderPropagationConfig
import de.otto.capella.headerpropagation.HeaderPropagationConfigs
import de.otto.capella.transformation.CodomainTransformationConfig
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
