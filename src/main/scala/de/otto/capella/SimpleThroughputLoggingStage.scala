package de.otto.capella

import com.typesafe.scalalogging.LazyLogging
import ox.Ox
import ox.flow.Flow
import ox.forkDiscard
import ox.scheduling.Schedule
import ox.scheduling.repeat

import scala.concurrent.duration.*

object SimpleThroughputLoggingStage extends LazyLogging:

    extension [A](source: Flow[A])
        def logThroughput(label: String, active: Option[Boolean])(using Ox): Flow[A] =
            if active.getOrElse(false) then
                var currentMinute: Int = 0
                var currentCount: Long = 0
                var currentMinuteStartCount: Long = 0

                forkDiscard:
                    repeat(Schedule.fixedInterval(1.minute)):
                        currentMinute = currentMinute + 1
                        val report =
                            s"Throughput at $label: ${currentCount - currentMinuteStartCount} messages per minute"
                        currentMinuteStartCount = currentCount
                        logger.info(report)

                source.tap: _ =>
                    currentCount = currentCount + 1
            else source
