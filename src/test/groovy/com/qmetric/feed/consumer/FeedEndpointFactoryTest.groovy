package com.qmetric.feed.consumer
import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.ClientHandlerException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spark.*
import spock.lang.Specification
import spock.lang.Timeout

import static java.util.concurrent.TimeUnit.SECONDS

class FeedEndpointFactoryTest extends Specification {

    private static final Logger log = LoggerFactory.getLogger(FeedEndpointFactoryTest)
    static timeout = new FeedEndpointFactory.ConnectionTimeout(SECONDS, 1)
    private static final int SERVER_PORT = 15001
    private static final String FEED_PATH = "/service-path"

    def setupSpec()
    {
        log.info("Setting up mock hal-feed-server")
        Spark.setPort(SERVER_PORT)
        Spark.get(new Route(FEED_PATH) {
            @Override Object handle(final Request request, final Response response)
            {
                log.info "Making the client wait 3 SECONDS"
                SECONDS.sleep(3)
                log.info "Returning"
                return null
            }
        })
    }

    def cleanupSpec()
    {
        SparkStopper.stop()
    }

    def "should create FeedEndpoint using factory"()
    {
        given:
        final client = Mock(Client)

        when:
        def feedEndpoint = new FeedEndpointFactory(client, timeout).create("any_url")

        then:
        1 * client.setConnectTimeout(_ as Integer)
        1 * client.setReadTimeout(_ as Integer)
        null != feedEndpoint
    }

    @Timeout(value = 10, unit = SECONDS) def 'throws SocketTimeoutException (read-timeout)'()
    {
        when:
        new FeedEndpointFactory(new Client(), timeout).create("http://localhost:${SERVER_PORT}${FEED_PATH}").get()

        then:
        def exception = thrown(ClientHandlerException)
        SocketTimeoutException.isAssignableFrom(exception.getCause().class)
    }

    @Timeout(value = 10, unit = SECONDS) def 'throws ConnectException'()
    {
        when:
        new FeedEndpointFactory(new Client(), timeout).create("http://localhost:15000").get()

        then:
        def exception = thrown(ClientHandlerException)
        ConnectException.isAssignableFrom(exception.getCause().class)
    }
}
