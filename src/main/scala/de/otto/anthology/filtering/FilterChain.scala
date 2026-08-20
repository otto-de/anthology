package de.otto.anthology.filtering

import com.fasterxml.jackson.databind.node.ArrayNode
import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.Option as JsonPathOption
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import de.otto.anthology.Message
import ox.computeIntensive

case class FilterChain(paths: Seq[JsonPath]):
    def apply(messageOpt: Option[Message]): Option[Message] =
        computeIntensive:
            paths.foldLeft(messageOpt)(applyFilter)

    private def applyFilter(messageOpt: Option[Message], filter: JsonPath): Option[Message] =
        messageOpt match
            case Some(message) =>
                val filterResults: ArrayNode =
                    FilterChain.context.parse(message.toJson).read[ArrayNode](filter)
                if filterResults.isEmpty then None
                else if filterResults.size == 1 then Some(Message(filterResults.get(0)))
                else
                    throw new IllegalArgumentException(
                        s"Possible misconfiguration: Multiple filter results when applying $filter on $message"
                    )
            case None =>
                None

object FilterChain:
    val context = JsonPath.using(
        Configuration
            .builder()
            .jsonProvider(JacksonJsonNodeJsonProvider())
            .options(JsonPathOption.ALWAYS_RETURN_LIST) // Result is always a list
            .options(JsonPathOption.SUPPRESS_EXCEPTIONS) // When no match: null instead of exception
            .build()
    )
