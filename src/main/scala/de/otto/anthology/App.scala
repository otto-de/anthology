package de.otto.anthology

import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.config.AnthologyConfig
import de.otto.anthology.config.AnthologyConfigFactory
import de.otto.anthology.config.CodomainConfig
import de.otto.anthology.config.CredentialsLoader
import de.otto.anthology.config.DomainConfigs
import de.otto.anthology.config.DomainRelationConfigs
import de.otto.anthology.config.KafkaClusterSettings
import de.otto.anthology.kafka.AggregateDeserializer
import de.otto.anthology.kafka.AggregateIdDeserializer
import de.otto.anthology.kafka.ClusterName
import de.otto.anthology.kafka.ConsumerMap
import de.otto.anthology.kafka.ConsumerName
import de.otto.anthology.statestore.RocksDBConfig
import de.otto.anthology.statestore.RocksDBStateStore
import de.otto.anthology.statestore.StateStore
import ox.Ox
import ox.OxApp
import ox.kafka.ConsumerSettings
import ox.kafka.ConsumerSettings.AutoOffsetReset
import ox.resilience.retry
import ox.scheduling.Schedule
import ox.supervised
import ox.useInScope

import scala.concurrent.duration.*
import scala.util.control.NonFatal

object App extends OxApp.Simple, LazyLogging:
    override def run(using Ox): Unit =
        retry(Schedule.fixedInterval(1.minute)):
            supervised:
                try
                    logger.info("Starting Anthology...")

                    // Setup infra...
                    val config: AnthologyConfig = AnthologyConfigFactory()
                    logger.info("Anthology configuration loaded successfully")

                    val credentials: Map[ClusterName, Map[String, String]] = CredentialsLoader()
                    logger.info("Credentials loaded successfully")

                    val clusterSettings: Map[ClusterName, KafkaClusterSettings] =
                        config.kafkaClusters.map(cc => cc.name -> KafkaClusterSettings(cc, credentials(cc.name))).toMap

                    val domainConfigs: DomainConfigs = DomainConfigs(config.domains)
                    val domainRelationConfigs: DomainRelationConfigs = DomainRelationConfigs(config.domainRelations)
                    val codomainConfig: CodomainConfig = config.codomain
                    logger.info("Domain and codomain settings initialized successfully")

                    val store: StateStore =
                        useInScope(aquireRocksDbStateStore(config.rocksDB))(releaseRocksDbStateStore)
                    logger.info("State store initialized successfully")

                    val kafkaConsumers: ConsumerMap =
                        domainConfigs.domains
                            .map: dConfig =>
                                val cluster = clusterSettings(dConfig.kafka.cluster)
                                val creds = cluster.credentials
                                val consumerSettings: ConsumerSettings[AggregateId, Option[Aggregate]] =
                                    ConsumerSettings
                                        .default(dConfig.kafka.consumerGroup)
                                        .bootstrapServers(cluster.config.bootstrapServers.split(",").map(_.trim)*)
                                        .keyDeserializer(AggregateIdDeserializer)
                                        .valueDeserializer(AggregateDeserializer)
                                        .autoOffsetReset(AutoOffsetReset.Earliest)
                                        .property("security.protocol", "SASL_SSL")
                                        .property("sasl.mechanism", "PLAIN")
                                        .property(
                                            "sasl.jaas.config",
                                            s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="${creds(
                                                    "username"
                                                )}" password="${creds("password")}";"""
                                        )
                                // for now, we go with consumer name == domain name
                                ConsumerName(dConfig.name.toString) -> consumerSettings.toThreadSafeConsumerWrapper
                            .toMap
                    logger.info("Kafka consumers initialized successfully")

                    // ...and run application
                    logger.info(s"Starting stream with ${config.parallelism}x parallelism...")
                    AppWorkflow.run(
                        domainConfigs,
                        domainRelationConfigs,
                        codomainConfig,
                        store,
                        clusterSettings,
                        kafkaConsumers,
                        config.parallelism
                    )
                catch
                    case NonFatal(e) =>
                        logger.error("Anthology failed with an exception. Will retry...", e)
                        throw e

    private def aquireRocksDbStateStore(dbConfig: RocksDBConfig): RocksDBStateStore =
        logger.info("Aquiring RocksDBStateStore...")
        RocksDBStateStore(dbConfig)

    private def releaseRocksDbStateStore(db: RocksDBStateStore): Unit =
        logger.info("Releasing RocksDBStateStore...")
        db.close()
