package de.otto.anthology.transformation

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import de.otto.anthology.Aggregate
import de.otto.anthology.JsonSupport
import io.joltcommunity.jolt.Chainr

object AggregateTransformer:

    private val mapper: ObjectMapper = JsonSupport.mapper
    private val typeref: TypeReference[Object] = new TypeReference {}

    def apply(aggregate: Aggregate, spec: Chainr): Aggregate =
        val aggregateJsonValue: Object =
            mapper.treeToValue(aggregate.toJson, typeref)
        val resultingJsonValue: Object = spec.transform(aggregateJsonValue)
        val resultingJsonTree: JsonNode = mapper.valueToTree(resultingJsonValue)
        Aggregate(resultingJsonTree)
