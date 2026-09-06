package de.otto.capella

import de.otto.capella.CodomainCompositionStage.composeCodomainMessages
import de.otto.capella.CodomainDeduplicationStage.deduplicateCodomainMessages
import de.otto.capella.CodomainInliningStage.inlineDomainMessages
import de.otto.capella.CodomainPersistenceStage.persistCodomainMessages
import de.otto.capella.CodomainTriggeringStage.triggerAffectedCodomainMessages
import de.otto.capella.DomainLinkingStage.linkDomainMessages
import de.otto.capella.DomainPersistenceStage.persistDomainMessages
import de.otto.capella.KafkaSink.emitCodomainMessages
import de.otto.capella.config.ChannelConfigs
import de.otto.capella.config.CodomainConfig
import de.otto.capella.config.KafkaClusterSettings
import de.otto.capella.config.RelationConfigs
import de.otto.capella.filtering.CodomainFilteringStage.filterCodomainMessages
import de.otto.capella.filtering.DomainFilteringStage.filterDomainMessages
import de.otto.capella.headerpropagation.HeaderPropagationStage.propagateHeaders
import de.otto.capella.kafka.ClusterName
import de.otto.capella.kafka.ConsumerMap
import de.otto.capella.statestore.StateStore
import de.otto.capella.transformation.CodomainTransformationStage.transformCodomainMessages
import de.otto.capella.transformation.DomainTransformationStage.transformDomainMessageIds
import de.otto.capella.transformation.DomainTransformationStage.transformDomainMessages
import ox.Ox

object AppWorkflow:

    /** Sets up and runs Capella's main application workflow.
      *
      * @param channelConfigs
      *   Configuration relating to the channels.
      * @param relationConfigs
      *   Configuration relating to the relations between the messages.
      * @param codomainConfig
      *   Codomain-related configuration.
      * @param stateStore
      *   A fully configured instance of the state store.
      */
    def run(
        channelConfigs: ChannelConfigs,
        relationConfigs: RelationConfigs,
        codomainConfig: CodomainConfig,
        stateStore: StateStore,
        clusterSettings: Map[ClusterName, KafkaClusterSettings],
        kafkaConsumers: ConsumerMap,
        parallelism: Parallelism,
        logDomainThroughput: Option[Boolean]
    )(using Ox): Unit =
        DomainSources(channelConfigs, kafkaConsumers, logDomainThroughput)
            .filterDomainMessages(channelConfigs, parallelism)
            .transformDomainMessageIds(channelConfigs)
            .transformDomainMessages(channelConfigs, parallelism)
            .persistDomainMessages(stateStore)
            .linkDomainMessages(relationConfigs, stateStore)
            .triggerAffectedCodomainMessages(relationConfigs, stateStore, parallelism)
            .deduplicateCodomainMessages(codomainConfig.deduplication)
            .composeCodomainMessages(stateStore)
            .inlineDomainMessages(relationConfigs, stateStore, parallelism)
            .filterCodomainMessages(codomainConfig.filtering, parallelism)
            .transformCodomainMessages(codomainConfig.transformation, parallelism)
            .persistCodomainMessages(stateStore)
            .propagateHeaders(codomainConfig.headerPropagationConfigs)
            .emitCodomainMessages(
                KafkaSinkSettings(
                    codomainConfig.kafka,
                    clusterSettings(codomainConfig.kafka.cluster),
                    kafkaConsumers,
                    codomainConfig.logSentMessages,
                    logDomainThroughput
                )
            )
