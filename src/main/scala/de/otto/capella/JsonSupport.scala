package de.otto.capella

import com.fasterxml.jackson.databind.ObjectMapper

object JsonSupport:
    lazy val mapper: ObjectMapper =
        val m: ObjectMapper = ObjectMapper()
        // Place for application-wide Jackson mapper config
        m
