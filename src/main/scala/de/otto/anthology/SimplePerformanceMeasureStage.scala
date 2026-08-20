package de.otto.anthology

import com.typesafe.scalalogging.LazyLogging
import ox.flow.Flow

import java.time.Duration
import java.time.Instant

object SimplePerformanceMeasureStage extends LazyLogging:

    private val ReportEveryNMessages = 1000

    extension [A](source: Flow[(Instant, A)])
        def measure(stage: String, enabled: Option[Boolean] = None): Flow[A] =
            if !enabled.getOrElse(false) then source.map(_._2)
            else
                var count: Long = 0
                var totalProcessingTimeMillis: Long = 0

                source
                    .tap: (startingTime, _) =>
                        val elapsedMillis = Duration.between(startingTime, Instant.now()).toMillis
                        totalProcessingTimeMillis = totalProcessingTimeMillis + elapsedMillis
                        count = count + 1
                        if count % ReportEveryNMessages == 0 then
                            val avgMillis = totalProcessingTimeMillis.toDouble / ReportEveryNMessages
                            logger.info(s"$stage: average processing time: ${avgMillis}ms")
                            totalProcessingTimeMillis = 0
                    .map(_._2)
