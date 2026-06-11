package de.otto.anthology

import de.otto.anthology.CodomainCompositionStage.composeCodomainAggregates
import de.otto.anthology.CodomainDeduplicationStage.deduplicateCodomainAggregates
import de.otto.anthology.CodomainInliningStage.inlineDomainAggregates
import de.otto.anthology.CodomainPersistenceStage.persistCodomainAggregates
import de.otto.anthology.CodomainTriggeringStage.triggerAffectedCodomainAggregates
import de.otto.anthology.DomainLinkingStage.linkDomainAggregates
import de.otto.anthology.DomainPersistenceStage.persistDomainAggregates
import de.otto.anthology.KafkaSink.emit
import de.otto.anthology.config.CodomainConfig
import de.otto.anthology.config.DomainConfigs
import de.otto.anthology.config.DomainRelationConfigs
import de.otto.anthology.config.KafkaClusterSettings
import de.otto.anthology.filtering.CodomainFilteringStage.filterCodomainAggregates
import de.otto.anthology.filtering.DomainFilteringStage.filterDomainAggregates
import de.otto.anthology.headerpropagation.HeaderPropagationStage.propagateHeaders
import de.otto.anthology.kafka.ClusterName
import de.otto.anthology.kafka.ConsumerMap
import de.otto.anthology.statestore.StateStore
import de.otto.anthology.transformation.CodomainTransformationStage.transformCodomainAggregates
import de.otto.anthology.transformation.DomainTransformationStage.transformDomainAggregateIds
import de.otto.anthology.transformation.DomainTransformationStage.transformDomainAggregates
import ox.Ox
import ox.channels.BufferCapacity

object AppWorkflow:

    given defaultBufferCapacity: BufferCapacity = BufferCapacity(4)

    /** Sets up and runs Anthology's main application workflow.
      *
      * @param domainConfigs
      *   Configuration relating to the domains.
      * @param domainRelationConfigs
      *   Configuration relating to the relations between the domains.
      * @param codomainConfig
      *   Codomain-related configuration.
      * @param stateStore
      *   A fully configured instance of the state store.
      */
    def run(
        domainConfigs: DomainConfigs,
        domainRelationConfigs: DomainRelationConfigs,
        codomainConfig: CodomainConfig,
        stateStore: StateStore,
        clusterSettings: Map[ClusterName, KafkaClusterSettings],
        kafkaConsumers: ConsumerMap,
        parallelism: Parallelism
    )(using Ox): Unit =
        DomainSources(domainConfigs, kafkaConsumers)
            .buffer()
            .filterDomainAggregates(domainConfigs, parallelism)
            .buffer()
            .transformDomainAggregateIds(domainConfigs)
            .buffer()
            .transformDomainAggregates(domainConfigs, parallelism)
            .buffer()
            .persistDomainAggregates(stateStore)
            .buffer()
            .linkDomainAggregates(domainRelationConfigs, stateStore, parallelism)
            .buffer()
            .triggerAffectedCodomainAggregates(domainRelationConfigs, stateStore, parallelism)
            .deduplicateCodomainAggregates(codomainConfig.deduplication)
            .composeCodomainAggregates(stateStore)
            .buffer()
            .inlineDomainAggregates(domainRelationConfigs, stateStore, parallelism)
            .buffer()
            .filterCodomainAggregates(codomainConfig.filtering, parallelism)
            .buffer()
            .transformCodomainAggregates(codomainConfig.transformation, parallelism)
            .buffer()
            .persistCodomainAggregates(stateStore, parallelism)
            .buffer()
            .buffer()
            .propagateHeaders(codomainConfig.headerPropagationConfigs)
            .emit(
                KafkaSinkSettings(
                    codomainConfig.kafka,
                    clusterSettings(codomainConfig.kafka.cluster),
                    kafkaConsumers
                )
            )
