package de.otto.anthology

import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.config.AnthologyConfig
import de.otto.anthology.config.AnthologyConfigFactory
import de.otto.anthology.config.CliConf
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
import ox.ExitCode
import ox.Ox
import ox.OxApp
import ox.discard
import ox.kafka.ConsumerSettings
import ox.kafka.ConsumerSettings.AutoOffsetReset
import ox.par
import ox.resilience.retry
import ox.scheduling.Schedule
import ox.supervised
import ox.useInScope

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import scala.concurrent.duration.*
import scala.util.control.NonFatal

object App extends OxApp, LazyLogging:
    override def run(args: Vector[String])(using Ox): ExitCode =
        retry(Schedule.fixedInterval(1.minute)):
            supervised:
                try
                    // Setup infra...
                    val cliConfig = CliConf(args)

                    val config: AnthologyConfig = AnthologyConfigFactory(cliConfig.anthologyConfigFile.toOption)
                    logger.info(s"Starting ${config.name}...")

                    val outboundIp: String = HttpClient
                        .newHttpClient()
                        .send(
                            HttpRequest.newBuilder(URI.create("https://api.ipify.org")).build(),
                            HttpResponse.BodyHandlers.ofString()
                        )
                        .body()
                    logger.info(s"Outbound IP is $outboundIp")

                    val credentials: Map[ClusterName, Map[String, String]] =
                        CredentialsLoader(cliConfig.anthologyCredentials.toOption)
                    logger.info("Credentials loaded successfully")

                    val clusterSettings: Map[ClusterName, KafkaClusterSettings] =
                        config.kafkaClusters.map(cc => cc.name -> KafkaClusterSettings(cc, credentials(cc.name))).toMap

                    val domainConfigs: DomainConfigs = DomainConfigs(config.domains)
                    val domainRelationConfigs: DomainRelationConfigs = DomainRelationConfigs(config.domainRelations)
                    val codomainConfig: CodomainConfig = config.codomain
                    logger.info("Domain and codomain settings initialized successfully")

                    val dbPath: String =
                        cliConfig.anthologyStateStorePath.getOrElse(sys.env("ANTHOLOGY_STATE_STORE_PATH"))
                    val dbConfig: RocksDBConfig = config.rocksDB
                    val store: StateStore =
                        useInScope(acquireRocksDbStateStore(dbConfig, dbPath))(releaseRocksDbStateStore)
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
                    def startHttpServer: Unit = Server().start()
                    def startAppWorkflow: Unit =
                        AppWorkflow.run(
                            domainConfigs,
                            domainRelationConfigs,
                            codomainConfig,
                            store,
                            clusterSettings,
                            kafkaConsumers,
                            config.parallelism
                        )
                    par(startHttpServer, startAppWorkflow).discard

                catch
                    case NonFatal(e) =>
                        logger.error("Anthology failed with an exception. Will retry...", e)
                        throw e
        ExitCode.Success

    private def acquireRocksDbStateStore(dbConfig: RocksDBConfig, dbPath: String): RocksDBStateStore =
        logger.info("Acquiring RocksDBStateStore...")
        RocksDBStateStore(dbConfig, dbPath)

    private def releaseRocksDbStateStore(db: RocksDBStateStore): Unit =
        logger.info("Releasing RocksDBStateStore...")
        db.close()
