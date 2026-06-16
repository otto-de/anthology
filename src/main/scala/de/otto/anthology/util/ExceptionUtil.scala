package de.otto.anthology.util

import java.io.PrintWriter
import java.io.StringWriter

object ExceptionUtil:

    extension [E <: Throwable](ex: E)
        def stackTraceAsString: String =
            val sw = new StringWriter()
            ex.printStackTrace(new PrintWriter(sw))
            sw.toString()
