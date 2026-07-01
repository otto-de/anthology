package de.otto.anthology.filtering

import com.jayway.jsonpath.JsonPath
import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.ChannelName
import de.otto.anthology.Message
import de.otto.anthology.MessageFormatName
import de.otto.anthology.Parallelism
import de.otto.anthology.QualifiedMessageId
import de.otto.anthology.config.ChannelConfigs
import de.otto.anthology.config.jsonPathConfigReader
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.util.ExceptionUtil.stackTraceAsString
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
