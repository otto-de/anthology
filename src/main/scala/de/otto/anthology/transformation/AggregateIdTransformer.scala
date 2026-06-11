package de.otto.anthology.transformation

import de.otto.anthology.AggregateId

import scala.util.matching.Regex

/** Transforms AggregateIds based on a given regex pattern. It expects exactly one match, otherwise it fails with an
  * exception. This one match may contain multiple parts (aka groups), which are all extracted and concatenated,
  * yielding to the resulting AggregateId.
  */
object AggregateIdTransformer:
    def apply(id: AggregateId, pattern: Regex): AggregateId =
        pattern.findFirstMatchIn(id.toString) match
            case Some(firstMatch) =>
                val matchedParts =
                    for i <- 1 to firstMatch.groupCount
                    yield firstMatch.group(i)
                AggregateId(matchedParts.mkString("_"))
            case None =>
                throw new IllegalArgumentException(s"Possible misconfiguration: Could not match $pattern in $id")
