package de.otto.anthology

import com.fasterxml.jackson.databind.node.ArrayNode
import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.Option as JsonPathOption
import com.jayway.jsonpath.ParseContext
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.config.AggregateConfig
import de.otto.anthology.config.DomainConfig
import de.otto.anthology.kafka.Passthrough
import ox.flow.Flow

import java.util.Objects

object DomainSource extends LazyLogging:
    def apply(
        config: DomainConfig,
        kafkaSettings: KafkaSourceSettings
    ): Flow[(Option[(QualifiedAggregateId, Option[Aggregate])], Passthrough)] =
        KafkaSource(kafkaSettings).map: pass =>
            val aggregateOpt: Option[Aggregate] = Option(pass.record.value).flatten
            val aggregateConfigOpt = recogniseAggregateConfig(aggregateOpt, config.aggregates)
            aggregateConfigOpt match
                case None =>
                    logger.debug(
                        s"Unable to recognise aggregate configuration for record (${pass.record.key}, ${pass.record.value})"
                    )
                    (None, pass)
                case Some(aggregateConfig) =>
                    val qaid = QualifiedAggregateId(config.name, aggregateConfig.name, pass.record.key)
                    (Some((qaid, aggregateOpt)), pass)

    private def recogniseAggregateConfig(
        aggregateOpt: Option[Aggregate],
        aggregateConfigs: Seq[AggregateConfig]
    ): Option[AggregateConfig] =
        if aggregateConfigs.size == 1 then aggregateConfigs.headOption
        else
            aggregateOpt match
                case None =>
                    None
                case Some(aggregate) =>
                    aggregateConfigs.find: aggConfig =>
                        aggConfig.recognitionPath match
                            case Some(recPath) =>
                                val result: ArrayNode = jsonPathContext.parse(aggregate.toJson).limit(1).read(recPath)
                                Objects.nonNull(result) && !result.isEmpty
                            case None =>
                                false

    private val jsonPathContext: ParseContext = JsonPath.using(
        Configuration
            .builder()
            .jsonProvider(JacksonJsonNodeJsonProvider())
            .options(JsonPathOption.SUPPRESS_EXCEPTIONS) // When no match: null instead of exception
            .build()
    )
