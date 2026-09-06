package de.otto.capella

import ox.channels.BufferCapacity
import ox.channels.ChannelClosed
import ox.channels.Source
import ox.channels.selectOrClosed
import ox.flow.Flow
import ox.repeatWhile
import ox.unsupervised

import scala.collection.immutable.Queue

object MergeStage:
    extension [A](sources: Iterable[Flow[A]])
        def merge(): Flow[A] = sources.reduce(_.merge(_))

        def mergeFair(propagateDone: Boolean = false)(using BufferCapacity): Flow[A] =
            assert(sources.nonEmpty)
            Flow.usingEmit: emit =>
                unsupervised:
                    var sourceChannels: Queue[Source[A]] = sources.map(_.runToChannel()).to(Queue)
                    repeatWhile:
                        val (head: Source[A], tail: Queue[Source[A]]) = sourceChannels.dequeue
                        sourceChannels = tail.enqueue(head)
                        selectOrClosed(sourceChannels) match
                            case ChannelClosed.Done =>
                                if !propagateDone then
                                    sourceChannels = sourceChannels.filterNot(_.isClosedForReceive)
                                    sourceChannels.nonEmpty
                                else false
                            case e: ChannelClosed.Error =>
                                throw e.toThrowable
                            case r: A @unchecked =>
                                emit(r)
                                true
