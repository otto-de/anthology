package de.otto.capella.transformation

import de.otto.capella.MessageId
import de.otto.capella.transformation.MessageIdTransformer
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AggregateIdTransformerTest extends AnyFlatSpec, Matchers, Diagrams:

    "AggregateIdTransformer" should "fail if the id is empty" in:
        assertThrows[IllegalArgumentException]:
            val id = MessageId("")
            val pat = "([0-9a-zA-Z-]+)_SomethingSilly_([0-9a-zA-Z-]+)".r
            MessageIdTransformer(id, pat)

    it should "fail if the id does not match" in:
        assertThrows[IllegalArgumentException]:
            val id = MessageId("abc-123_Something_cde-789")
            val pat = "([0-9a-zA-Z-]+)_SomethingSilly_([0-9a-zA-Z-]+)".r
            MessageIdTransformer(id, pat)

    it should "match and extract one part" in:
        // given
        val id = MessageId("SomethingSilly_cde-789")
        val pat = "SomethingSilly_([0-9a-zA-Z-]+)".r

        // when
        val result = MessageIdTransformer(id, pat)

        // then
        val expected = MessageId("cde-789")
        assert(result == expected)

    it should "match and extract multiple parts" in:
        // given
        val id = MessageId("abc-123_SomethingSilly_cde-789")
        val pat = "([0-9a-zA-Z-]+)_SomethingSilly_([0-9a-zA-Z-]+)".r

        // when
        val result = MessageIdTransformer(id, pat)

        // then
        val expected = MessageId("abc-123_cde-789")
        assert(result == expected)
