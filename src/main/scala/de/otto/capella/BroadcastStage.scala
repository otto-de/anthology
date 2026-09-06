package de.otto.capella

import ox.channels.Sink
import ox.flow.Flow

object BroadcastStage:
    extension [T](source: Flow[T])
        def broadcast(sinks: Iterable[Sink[T]]): Flow[T] =
            sinks.foldLeft(source)((src, snk) => src.alsoTo(snk))
