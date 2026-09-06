package de.otto.capella.filtering

import com.jayway.jsonpath.JsonPath
import com.typesafe.scalalogging.LazyLogging
import de.otto.capella.ChannelName
import de.otto.capella.Message
import de.otto.capella.MessageFormatName
import de.otto.capella.Parallelism
import de.otto.capella.QualifiedMessageId
import de.otto.capella.SimpleProcessingTimeLogger.measureMap
import de.otto.capella.config.ChannelConfigs
import de.otto.capella.config.jsonPathConfigReader
import de.otto.capella.kafka.Passthrough
import de.otto.capella.util.ExceptionUtil.stackTraceAsString
import ox.flow.Flow
import pureconfig.ConfigReader

import scala.util.control.NonFatal

object DomainFilteringStage extends LazyLogging:

    extension (in: Flow[(Option[(QualifiedMessageId, Option[Message])], Passthrough)])
        def filterDomainMessages(
            configs: ChannelConfigs,
            parallelism: Parallelism = Parallelism(1)
        ): Flow[(Option[(QualifiedMessageId, Option[Message])], Passthrough)] =
            val chains: Map[(ChannelName, MessageFormatName), FilterChain] =
                configs.messageFormatsByName.flatMap: (chanName2msgName, msgConfig) =>
                    msgConfig.filtering.map(fc => (chanName2msgName, FilterChain(fc.filterPaths)))

            in.mapPar(parallelism.toInt):
                measureMap("DomainFiltering"):
                    case (None, pass) =>
                        (None, pass)

                    case (Some(qmid, messageOpt), pass) =>
                        try
                            val chainOpt = chains.get((qmid.channelName, qmid.messageName))
                            val filteredDomainMessage =
                                chainOpt match
                                    case Some(chain) =>
                                        chain(messageOpt)
                                    case None =>
                                        messageOpt
                            (Some(qmid, filteredDomainMessage), pass)
                        catch
                            case NonFatal(ex) =>
                                logger.error(
                                    s"Error processing record (${pass.record.key}, ${pass.record.value}): ${ex.stackTraceAsString}"
                                )
                                (None, pass)

case class DomainFilteringConfig(filterPaths: Seq[JsonPath]) derives ConfigReader
