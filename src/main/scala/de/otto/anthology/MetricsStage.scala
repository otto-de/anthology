package de.otto.anthology

import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.Meter
import ox.flow.Flow

object MetricsStage:
    extension [A](source: Flow[A])
        def counter(meter: Meter, config: MetricsStageConfig): Flow[A] =
            lazy val counter: LongCounter =
                meter
                    .counterBuilder(config.name)
                    .setDescription(config.description)
                    .build()
            source.tap(_ => counter.add(1L))

case class MetricsStageConfig(name: String, description: String)
