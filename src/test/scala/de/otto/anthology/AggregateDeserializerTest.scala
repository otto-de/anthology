package de.otto.anthology

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import de.otto.anthology.kafka.AggregateDeserializer
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AggregateDeserializerTest extends AnyFlatSpec, Matchers, Diagrams:

    "AggregateDeserializer" should "deserialize non-empty message" in:
        // given
        val jsonGiven = """{"foo":"bar1"}""".getBytes

        // when
        val result = AggregateDeserializer.deserialize("topic", jsonGiven)

        // then
        val nf = JsonNodeFactory.instance
        val expected_map: java.util.Map[String, JsonNode] = java.util.HashMap()
        expected_map.put("foo", TextNode("bar1"))
        val expected = Some(Aggregate(ObjectNode(nf, expected_map)))

        assert(expected == result)

    it should "deserialize empty message" in:
        // given
        val jsonGiven = "".getBytes

        // when
        val result = AggregateDeserializer.deserialize("topic", jsonGiven)

        // then
        val expected = None

        assert(expected == result)

    it should "deserialize null message" in:
        // given when
        val result = AggregateDeserializer.deserialize("topic", null) // scalafix:ok

        // then
        val expected = None

        assert(expected == result)
