package de.otto.anthology

import de.otto.anthology.CodomainCompositionStage.composeCodomainMessages
import de.otto.anthology.CodomainDeduplicationStage.deduplicateCodomainMessages
import de.otto.anthology.CodomainInliningStage.inlineDomainMessages
import de.otto.anthology.CodomainPersistenceStage.persistCodomainMessages
import de.otto.anthology.CodomainTriggeringStage.triggerAffectedCodomainMessages
import de.otto.anthology.DomainLinkingStage.linkDomainMessages
import de.otto.anthology.DomainPersistenceStage.persistDomainMessages
import de.otto.anthology.KafkaSink.emitCodomainMessages
import de.otto.anthology.config.ChannelConfigs
import de.otto.anthology.config.CodomainConfig
import de.otto.anthology.config.KafkaClusterSettings
import de.otto.anthology.config.RelationConfigs
import de.otto.anthology.filtering.CodomainFilteringStage.filterCodomainMessages
import de.otto.anthology.filtering.DomainFilteringStage.filterDomainMessages
import de.otto.anthology.headerpropagation.HeaderPropagationStage.propagateHeaders
import de.otto.anthology.kafka.ClusterName
import de.otto.anthology.kafka.ConsumerMap
import de.otto.anthology.statestore.StateStore
import de.otto.anthology.transformation.CodomainTransformationStage.transformCodomainMessages
import de.otto.anthology.transformation.DomainTransformationStage.transformDomainMessageIds
import de.otto.anthology.transformation.DomainTransformationStage.transformDomainMessages
import ox.Ox
import ox.channels.BufferCapacity

object AppWorkflow:

    given defaultBufferCapacity: BufferCapacity = BufferCapacity(4)

    /** Sets up and runs Anthology's main application workflow.
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
        parallelism: Parallelism
    )(using Ox): Unit =
        DomainSources(channelConfigs, kafkaConsumers)
            .buffer()
            .filterDomainMessages(channelConfigs, parallelism)
            .buffer()
            .transformDomainMessageIds(channelConfigs)
            .buffer()
            .transformDomainMessages(channelConfigs, parallelism)
            .buffer()
            .persistDomainMessages(stateStore)
            .buffer()
            .linkDomainMessages(relationConfigs, stateStore)
            .buffer()
            .triggerAffectedCodomainMessages(relationConfigs, stateStore, parallelism)
            .deduplicateCodomainMessages(codomainConfig.deduplication)
            .composeCodomainMessages(stateStore)
            .buffer()
            .inlineDomainMessages(relationConfigs, stateStore, parallelism)
            .buffer()
            .filterCodomainMessages(codomainConfig.filtering, parallelism)
            .buffer()
            .transformCodomainMessages(codomainConfig.transformation, parallelism)
            .buffer()
            .persistCodomainMessages(stateStore, parallelism)
            .buffer()
            .propagateHeaders(codomainConfig.headerPropagationConfigs)
            .buffer()
            .emitCodomainMessages(
                KafkaSinkSettings(
                    codomainConfig.kafka,
                    clusterSettings(codomainConfig.kafka.cluster),
                    kafkaConsumers,
                    codomainConfig.logSentMessages
                )
            )
