package de.otto.anthology

import com.typesafe.scalalogging.LazyLogging

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import scala.collection.concurrent.TrieMap

object SimpleProcessingTimeLogger extends LazyLogging:

    var reportEveryNMessages: Int = 10_000

    var reportToFile: Option[Path] = None

    private val measurements: TrieMap[String, Measurement] = TrieMap.empty

    /** Measures the average processing time of a map operation.
      */
    def measureMap[T, U](label: String)(f: T => U): T => U =
        t =>
            val start = System.nanoTime()
            val result = f(t)
            val durationMicros = (System.nanoTime() - start) / 1000
            synchronized:
                val last = measurements.getOrElse(label, Measurement())
                val next = Measurement(last.totalDuration + durationMicros, last.count + 1)
                if next.count % reportEveryNMessages == 0 then
                    val avgMicros = next.totalDuration / next.count
                    logger.info(s"$label average processing time: ${avgMicros}µs")
                    reportToFile.foreach: filePath =>
                        Files.writeString(
                            filePath,
                            s"$label;${System.currentTimeMillis};$avgMicros\n",
                            StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.APPEND
                        )
                    measurements.remove(label)
                else measurements.update(label, next)
            result

    private case class Measurement(totalDuration: Long = 0, count: Long = 0)
