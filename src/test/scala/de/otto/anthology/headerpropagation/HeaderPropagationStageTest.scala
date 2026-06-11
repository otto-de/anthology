package de.otto.anthology.headerpropagation

import de.otto.anthology.AggregateId
import de.otto.anthology.TestUtils.mockedEmptyKafkaRecord
import de.otto.anthology.headerpropagation.GenerateConstant
import de.otto.anthology.headerpropagation.GenerateTimestamp
import de.otto.anthology.headerpropagation.GenerateUUID
import de.otto.anthology.headerpropagation.HeaderPropagationConfigs
import de.otto.anthology.headerpropagation.HeaderPropagationStage.propagateHeaders
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.flow.Flow

import java.nio.charset.StandardCharsets
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

class HeaderPropagationStageTest extends AnyFlatSpec, Matchers, Diagrams:
    "HeaderPropagationStage" should "generate headers" in:
        // given
        val aggIdX = AggregateId("abc-def-123")
        val aggX = None
        val passX = mockedEmptyKafkaRecord("abc-def-123")
        val configs =
            Some(
                HeaderPropagationConfigs(
                    Seq(
                        GenerateConstant("hello", "world"),
                        GenerateConstant("hello2", "world2"),
                        GenerateTimestamp("updated"),
                        GenerateUUID("id")
                    )
                )
            )

        // when
        val src = Flow.fromValues(
            (Seq((aggIdX, aggX)), Seq(passX))
        )
        val result = src.propagateHeaders(configs).runToList()

        // then
        val actualHeaders = result.head._1.head._3.get.iterator.asScala.toSeq
        assert(actualHeaders.size == 4)
        assert(actualHeaders(0).key == "hello")
        assert(new String(actualHeaders(0).value, StandardCharsets.UTF_8) == "world")
        assert(actualHeaders(1).key == "hello2")
        assert(new String(actualHeaders(1).value, StandardCharsets.UTF_8) == "world2")
        assert(actualHeaders(2).key == "updated")
        assert(
            OffsetDateTime
                .parse(new String(actualHeaders(2).value, StandardCharsets.UTF_8), ISO_OFFSET_DATE_TIME)
                .getDayOfYear() == OffsetDateTime.now(ZoneId.of("UTC")).getDayOfYear()
        )
        val uuidPattern: Regex =
            """^[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12}$""".r
        assert(actualHeaders(3).key == "id")
        assert(uuidPattern.matches(new String(actualHeaders(3).value, StandardCharsets.UTF_8)))
