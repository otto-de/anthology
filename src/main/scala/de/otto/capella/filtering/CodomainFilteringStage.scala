package de.otto.capella.filtering

import com.jayway.jsonpath.JsonPath
import com.typesafe.scalalogging.LazyLogging
import de.otto.capella.Message
import de.otto.capella.MessageId
import de.otto.capella.Parallelism
import de.otto.capella.SimpleProcessingTimeLogger.measureMap
import de.otto.capella.config.jsonPathConfigReader
import de.otto.capella.kafka.Passthrough
import de.otto.capella.util.ExceptionUtil.stackTraceAsString
import ox.flow.Flow
import pureconfig.ConfigReader

import scala.util.control.NonFatal

object CodomainFilteringStage extends LazyLogging:

    extension (in: Flow[(Seq[(MessageId, Option[Message])], Seq[Passthrough])])
        def filterCodomainMessages(
            configOpt: Option[CodomainFilteringConfig],
            parallelism: Parallelism = Parallelism(1)
        ): Flow[(Seq[(MessageId, Option[Message])], Seq[Passthrough])] =
            val chain: FilterChain = FilterChain(configOpt.map(_.filterPaths).getOrElse(Seq.empty))
            in.mapPar(parallelism.toInt):
                measureMap("CodomainFiltering"): (payloads, passthroughs) =>
                    val payloadsOut: Seq[(MessageId, Option[Message])] =
                        payloads.map: (codomainMessageId, codomainMessage) =>
                            try (codomainMessageId, chain(codomainMessage))
                            catch
                                case NonFatal(ex) =>
                                    logger.error(
                                        s"Error filtering ($codomainMessageId, $codomainMessage): ${ex.stackTraceAsString}"
                                    )
                                    (codomainMessageId, None)
                    (payloadsOut, passthroughs)

case class CodomainFilteringConfig(filterPaths: Seq[JsonPath]) derives ConfigReader
