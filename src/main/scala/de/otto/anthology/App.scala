package de.otto.anthology

import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.config.AdditionalKafkaPropertiesLoader
import de.otto.anthology.config.AnthologyConfig
import de.otto.anthology.config.AnthologyConfigFactory
import de.otto.anthology.config.AppArgs
import de.otto.anthology.config.ChannelConfigs
import de.otto.anthology.config.CliConf
import de.otto.anthology.config.CodomainConfig
import de.otto.anthology.config.KafkaClusterSettings
import de.otto.anthology.config.RelationConfigs
import de.otto.anthology.http.Server
import de.otto.anthology.kafka.ClusterName
import de.otto.anthology.kafka.ConsumerMap
import de.otto.anthology.kafka.ConsumerName
import de.otto.anthology.kafka.MessageDeserializer
import de.otto.anthology.kafka.MessageIdDeserializer
import de.otto.anthology.statestore.RocksDBConfig
import de.otto.anthology.statestore.RocksDBStateStore
import de.otto.anthology.statestore.StateStore
import de.otto.anthology.util.ExceptionUtil.stackTraceAsString
import org.rocksdb.RocksDBException
import ox.ExitCode
import ox.Ox
import ox.OxApp
import ox.discard
import ox.kafka.ConsumerSettings
import ox.kafka.ConsumerSettings.AutoOffsetReset
import ox.par
import ox.resilience.ResultPolicy
import ox.resilience.RetryConfig
import ox.resilience.retry
import ox.scheduling.Schedule
import ox.supervised
import ox.useInScope

import scala.concurrent.duration.*
import scala.util.control.NonFatal

object App extends OxApp, LazyLogging:

    private def retrySchedule: Schedule = Schedule.fixedInterval(1.minute)

    private def retryPolicy: ResultPolicy[Throwable, Unit] =
        ResultPolicy.retryWhen:
            case _: RocksDBException => false
            case NonFatal(e) =>
                logger.error("Nonfatal error occured, will try to recover:", e)
                true
            case _ => false

    override def run(args: Vector[String])(using Ox): ExitCode =
        try
            retry(RetryConfig(retrySchedule, retryPolicy)):
                supervised:
                    // Setup infra...
                    val cliConfig = CliConf(args)

                    val cpuCount: Int = Runtime.getRuntime.availableProcessors()

                    val config: AnthologyConfig = AnthologyConfigFactory(cliConfig.anthologyConfigFile.toOption)
                    logger.info(s"Starting ${config.name} with $cpuCount CPUs...")

                    val additionalKafkaProps: Map[ClusterName, Map[String, String]] =
                        AdditionalKafkaPropertiesLoader(cliConfig.anthologyAdditionalKafkaProperties.toOption)
                    logger.info("Additional Kafka properties loaded successfully")

                    val clusterSettings: Map[ClusterName, KafkaClusterSettings] =
                        config.kafkaClusters
                            .map(cc => cc.name -> KafkaClusterSettings(cc, additionalKafkaProps(cc.name)))
                            .toMap

                    val channelConfigs: ChannelConfigs = ChannelConfigs(config.domain.channels)
                    val relationConfigs: RelationConfigs = RelationConfigs(config.domain.relations)
                    val codomainConfig: CodomainConfig = config.codomain
                    logger.info("Domain and codomain settings initialized successfully")

                    val dbPath: String =
                        cliConfig.anthologyStateStorePath.getOrElse(sys.env(AppArgs.STATE_STORE_PATH_ENV_VAR))
                    val dbConfig: RocksDBConfig = config.rocksDB
                    val store: StateStore =
                        useInScope(acquireRocksDbStateStore(dbConfig, dbPath))(releaseRocksDbStateStore)
                    logger.info("State store initialized successfully")

                    val kafkaConsumers: ConsumerMap =
                        channelConfigs.channels
                            .map: dConfig =>
                                val cluster = clusterSettings(dConfig.kafka.cluster)
                                val additionalProps =
                                    cluster.additionalProperties ++ dConfig.kafka.additionalConsumerPropertiesAsMap
                                val baseSettings: ConsumerSettings[MessageId, Option[Message]] =
                                    ConsumerSettings
                                        .default(dConfig.kafka.consumerGroup)
                                        .bootstrapServers(cluster.config.bootstrapServers.split(",").map(_.trim)*)
                                        .keyDeserializer(MessageIdDeserializer)
                                        .valueDeserializer(MessageDeserializer)
                                        .autoOffsetReset(AutoOffsetReset.Earliest)
                                val consumerSettings: ConsumerSettings[MessageId, Option[Message]] =
                                    additionalProps.foldLeft(baseSettings)((s, k2v) => s.property(k2v._1, k2v._2))
                                // for now, we go with consumer name == domain name
                                ConsumerName(dConfig.name.toString) -> consumerSettings.toThreadSafeConsumerWrapper
                            .toMap
                    logger.info("Kafka consumers initialized successfully")

                    // ...and run application
                    logger.info("Starting processing...")
                    def startHttpServer(): Unit = Server().start()
                    def startAppWorkflow(): Unit =
                        AppWorkflow.run(
                            channelConfigs,
                            relationConfigs,
                            codomainConfig,
                            store,
                            clusterSettings,
                            kafkaConsumers,
                            Parallelism(cpuCount),
                            config.domain.logThroughput
                        )
                    par(startHttpServer(), startAppWorkflow()).discard
            ExitCode.Success
        catch
            case ex: RocksDBException =>
                logger.error(s"Anthology is shutting down due to a database error: ${ex.stackTraceAsString}")
                ExitCode.Failure(10)
            case ex =>
                logger.error(s"Anthology is shutting down due to a serious error: ${ex.stackTraceAsString}")
                ExitCode.Failure(1)

    private def acquireRocksDbStateStore(dbConfig: RocksDBConfig, dbPath: String): RocksDBStateStore =
        RocksDBStateStore(dbConfig, dbPath)

    private def releaseRocksDbStateStore(db: RocksDBStateStore): Unit =
        db.shutdown()
