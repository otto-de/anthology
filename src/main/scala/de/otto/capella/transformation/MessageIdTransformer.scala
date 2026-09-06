package de.otto.capella.transformation

import de.otto.capella.MessageId

import scala.util.matching.Regex

/** Transforms MessageIds based on a given regex pattern. It expects exactly one match, otherwise it fails with an
  * exception. This one match may contain multiple parts (aka groups), which are all extracted and concatenated,
  * yielding to the resulting MessageId.
  */
object MessageIdTransformer:
    def apply(id: MessageId, pattern: Regex): MessageId =
        pattern.findFirstMatchIn(id.toString) match
            case Some(firstMatch) =>
                val matchedParts =
                    for i <- 1 to firstMatch.groupCount
                    yield firstMatch.group(i)
                MessageId(matchedParts.mkString("_"))
            case None =>
                throw new IllegalArgumentException(s"Possible misconfiguration: Could not match $pattern in $id")
