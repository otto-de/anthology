package de.otto.anthology

import de.otto.anthology.MetricsStage.*
import de.otto.anthology.MetricsStageConfig
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.flow.Flow

import scala.jdk.CollectionConverters.*
import scala.util.Random

class MetricsStageTest extends AnyFlatSpec, Matchers, Diagrams:

    val sdkMeterReader = InMemoryMetricReader.create()
    val sdkMeterProvider = SdkMeterProvider.builder().registerMetricReader(sdkMeterReader).build()
    val sdkMeter = sdkMeterProvider.get(getClass().getName())

    "MetricsStage" should "count the elements flowing through" in:
        // given
        val inputValues = List.fill(99)(Random.nextString(5))
        val input: Flow[String] = Flow.fromIterable(inputValues)

        // when
        val config = MetricsStageConfig("test", "test")
        val flow = input.counter(sdkMeter, config)
        val outputValues = flow.runToList()

        // then
        // elements passed unchanged:
        assert(inputValues == outputValues)

        // elements counted correctly:
        assert(
            sdkMeterReader
                .collectAllMetrics()
                .asScala
                .exists(
                    _.getLongSumData()
                        .getPoints()
                        .asScala
                        .exists(_.getValue() == inputValues.size.toLong)
                )
        )
