package de.otto.anthology

import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.config.AnthologyConfig
import de.otto.anthology.config.AnthologyConfigFactory
import de.otto.anthology.config.CodomainConfig
import de.otto.anthology.config.CredentialsLoader
import de.otto.anthology.config.DomainConfigs
import de.otto.anthology.config.DomainRelationConfigs
import de.otto.anthology.config.KafkaClusterSettings
import de.otto.anthology.http.Server
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
import ox.fork
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
                    logger.info("Starting Anthology (v0.0.6)...")

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
                        useInScope(acquireRocksDbStateStore(config.rocksDB))(releaseRocksDbStateStore)
                    logger.info("State store initialized successfully")

                    val kafkaConsumers: ConsumerMap =
                        domainConfigs.domains
                            .map: dConfig =>
                                val cluster = clusterSettings(dConfig.kafka.cluster)
                                val creds = cluster.credentials
                                val baseSettings: ConsumerSettings[AggregateId, Option[Aggregate]] =
                                    ConsumerSettings
                                        .default(dConfig.kafka.consumerGroup)
                                        .bootstrapServers(cluster.config.bootstrapServers.split(",").map(_.trim)*)
                                        .keyDeserializer(AggregateIdDeserializer)
                                        .valueDeserializer(AggregateDeserializer)
                                        .autoOffsetReset(AutoOffsetReset.Earliest)
                                val consumerSettings: ConsumerSettings[AggregateId, Option[Aggregate]] =
                                    creds.foldLeft(baseSettings)((s, k2v) => s.property(k2v._1, k2v._2))
                                // for now, we go with consumer name == domain name
                                ConsumerName(dConfig.name.toString) -> consumerSettings.toThreadSafeConsumerWrapper
                            .toMap
                    logger.info("Kafka consumers initialized successfully")

                    // ...and run application
                    logger.info(s"Starting stream with ${config.parallelism}x parallelism...")

                    val serverF = fork:
                        Server().start()

                    val appF = fork:
                        AppWorkflow.run(
                            domainConfigs,
                            domainRelationConfigs,
                            codomainConfig,
                            store,
                            clusterSettings,
                            kafkaConsumers,
                            config.parallelism
                        )

                    (serverF.join(), appF.join())
                catch
                    case NonFatal(e) =>
                        logger.error("Anthology failed with an exception. Will retry...", e)
                        throw e

    private def acquireRocksDbStateStore(dbConfig: RocksDBConfig): RocksDBStateStore =
        logger.info("Acquiring RocksDBStateStore...")
        RocksDBStateStore(dbConfig)

    private def releaseRocksDbStateStore(db: RocksDBStateStore): Unit =
        logger.info("Releasing RocksDBStateStore...")
        db.close()
