package com.qmetric.feed.consumer

import spock.lang.Specification

import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit

class ShutdownProcedureTest extends Specification
{

    def service = Mock(ExecutorService)

    def shutdown = new ShutdownProcedure(service)

    def 'stops executor-services and waits jobs to terminate'()
    {
        when:
        shutdown.run()
        then:
        1 * service.shutdown()
        1 * service.awaitTermination(_ as Long, _ as TimeUnit) >> true
    }

    def 'forces shutdown if jobs fail to terminate'()
    {
        when:
        shutdown.run()
        then:
        1 * service.shutdown()
        1 * service.awaitTermination(_ as Long, _ as TimeUnit) >> false
        1 * service.shutdownNow()
    }

    def 'forces shutdown if jobs termination throws exception'()
    {
        when:
        shutdown.run()
        then:
        1 * service.shutdown()
        1 * service.awaitTermination(_ as Long, _ as TimeUnit) >> { throw new InterruptedException() }
        1 * service.shutdownNow()
    }

    def 'does not attempt shutdown if executor is already shutdown'()
    {
        when:
        shutdown.run()
        then:
        1 * service.isShutdown() >> true
        0 * service.shutdown()
        1 * service.awaitTermination(_ as Long, _ as TimeUnit) >> true
    }

    def 'does not attempt job termination if executor is already terminated'()
    {
        when:
        shutdown.run()
        then:
        1 * service.isTerminated() >> true
        0 * service.awaitTermination(_ as Long, _ as TimeUnit) >> true
    }
}
