package de.otto.anthology

import com.typesafe.scalalogging.LazyLogging
import ox.Ox
import ox.flow.Flow
import ox.forkDiscard
import ox.scheduling.Schedule
import ox.scheduling.repeat

import scala.concurrent.duration.*

object SimpleLoggingCounterStage extends LazyLogging:

    private val hourFactor: Int = 60
    private val dayFactor: Int = hourFactor * 24

    extension [A](source: Flow[A])
        def count(name: String)(using Ox): Flow[A] =
            var currentMinute: Int = 0
            var currentCount: Long = 0
            var currentMinuteStartCount: Long = 0
            var currentHourStartCount: Long = 0
            var currentDayStartCount: Long = 0

            forkDiscard:
                repeat(Schedule.fixedInterval(1.minute)):
                    currentMinute = currentMinute + 1
                    val report = StringBuilder(s"Throughputs at $name \n")
                    report.append(s"   - Last minute: ${currentCount - currentMinuteStartCount} \n")
                    currentMinuteStartCount = currentCount
                    if currentMinute % hourFactor == 0 then
                        report.append(s"   - Last hour:   ${currentCount - currentHourStartCount} \n")
                        currentHourStartCount = currentCount
                    if currentMinute % dayFactor == 0 then
                        report.append(s"   - Last day:    ${currentCount - currentDayStartCount} \n")
                        currentDayStartCount = currentCount
                    logger.info(report.toString)

            source.tap: _ =>
                currentCount = currentCount + 1
