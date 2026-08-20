package de.otto.anthology

import com.typesafe.scalalogging.LazyLogging

import java.time.Duration
import java.time.Instant
import scala.collection.concurrent.TrieMap

object SimpleProcessingTimeLogger extends LazyLogging:

    private val reportEveryNMessages: Int = 1000

    private val measurements: TrieMap[String, Measurement] = TrieMap.empty

    def measure[T](label: String)(f: => T): T =
        val start = Instant.now()
        val result = f
        val duration = Duration.between(start, Instant.now()).toMillis
        synchronized:
            val last = measurements.getOrElse(label, Measurement())
            val next = Measurement(last.totalDuration + duration, last.count + 1)
            if next.count % reportEveryNMessages == 0 then
                val avgMillis = next.totalDuration.toDouble / next.count
                logger.info(s"$label: average processing time: ${avgMillis}ms")
                measurements.remove(label)
            else measurements.update(label, next)
        result

    private case class Measurement(totalDuration: Long = 0, count: Long = 0)
