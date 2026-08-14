package de.otto.anthology.transformation

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import de.otto.anthology.JsonSupport
import de.otto.anthology.Message
import io.joltcommunity.jolt.Chainr
import ox.computeIntensive

object MessageTransformer:

    private val mapper: ObjectMapper = JsonSupport.mapper
    private val typeref: TypeReference[Object] = new TypeReference {}

    def apply(message: Message, spec: Chainr): Message =
        computeIntensive:
            val messageJsonValue: Object =
                mapper.treeToValue(message.toJson, typeref)
            val resultingJsonValue: Object = spec.transform(messageJsonValue)
            val resultingJsonTree: JsonNode = mapper.valueToTree(resultingJsonValue)
            Message(resultingJsonTree)
