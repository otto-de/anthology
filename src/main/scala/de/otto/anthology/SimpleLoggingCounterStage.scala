package de.otto.anthology

import com.typesafe.scalalogging.LazyLogging
import ox.Ox
import ox.flow.Flow
import ox.forkDiscard
import ox.scheduling.Schedule
import ox.scheduling.repeat

import scala.concurrent.duration.*

object SimpleLoggingCounterStage extends LazyLogging:

    extension [A](source: Flow[A])
        def count(name: String)(using Ox): Flow[A] =
            var currentMinute: Int = 0
            var currentCount: Long = 0
            var currentMinuteStartCount: Long = 0

            forkDiscard:
                repeat(Schedule.fixedInterval(1.minute)):
                    currentMinute = currentMinute + 1
                    val report = s"Throughput at $name: ${currentCount - currentMinuteStartCount} messages per minute"
                    currentMinuteStartCount = currentCount
                    logger.info(report)

            source.tap: _ =>
                currentCount = currentCount + 1
