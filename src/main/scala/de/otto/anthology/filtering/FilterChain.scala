package de.otto.anthology.filtering

import com.fasterxml.jackson.databind.node.ArrayNode
import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.Option as JsonPathOption
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import de.otto.anthology.Aggregate

case class FilterChain(paths: Seq[JsonPath]):
    def apply(aggregateOpt: Option[Aggregate]): Option[Aggregate] =
        paths.foldLeft(aggregateOpt)(applyFilter)

    private def applyFilter(aggregateOpt: Option[Aggregate], filter: JsonPath): Option[Aggregate] =
        aggregateOpt match
            case Some(aggregate) =>
                val filterResults: ArrayNode =
                    FilterChain.context.parse(aggregate.toJson).read[ArrayNode](filter)
                if filterResults.isEmpty then None
                else if filterResults.size == 1 then Some(Aggregate(filterResults.get(0)))
                else
                    throw new IllegalArgumentException(
                        s"Possible misconfiguration: Multiple filter results when applying $filter on $aggregate"
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
