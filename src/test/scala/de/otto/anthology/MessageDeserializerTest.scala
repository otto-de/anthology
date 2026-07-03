package de.otto.anthology

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import de.otto.anthology.kafka.MessageDeserializer
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MessageDeserializerTest extends AnyFlatSpec, Matchers, Diagrams:

    "MessageDeserializer" should "deserialize non-empty message" in:
        // given
        val jsonGiven = """{"foo":"bar1"}""".getBytes

        // when
        val result = MessageDeserializer.deserialize("topic", jsonGiven)

        // then
        val nf = JsonNodeFactory.instance
        val expected_map: java.util.Map[String, JsonNode] = java.util.HashMap()
        expected_map.put("foo", TextNode("bar1"))
        val expected = Some(Message(ObjectNode(nf, expected_map)))

        assert(expected == result)

    it should "deserialize empty message" in:
        // given
        val jsonGiven = "".getBytes

        // when
        val result = MessageDeserializer.deserialize("topic", jsonGiven)

        // then
        val expected = None

        assert(expected == result)

    it should "deserialize null message" in:
        // given when
        val result = MessageDeserializer.deserialize("topic", null) // scalafix:ok

        // then
        val expected = None

        assert(expected == result)
