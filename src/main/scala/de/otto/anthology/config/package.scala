package de.otto.anthology.config

import com.jayway.jsonpath.JsonPath
import pureconfig.ConfigReader

given jsonPathConfigReader: ConfigReader[JsonPath] = ConfigReader[String].map(path => JsonPath.compile(path))
