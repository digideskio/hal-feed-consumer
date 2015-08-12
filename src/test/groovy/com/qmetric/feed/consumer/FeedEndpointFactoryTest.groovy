package com.qmetric.feed.consumer

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spark.*
import spock.lang.Specification
import spock.lang.Timeout

import static java.util.concurrent.TimeUnit.SECONDS

class FeedEndpointFactoryTest extends Specification
{
    private static final Logger LOG = LoggerFactory.getLogger(FeedEndpointFactoryTest)
    private static final int SERVER_PORT = 15001
    private static final String FEED_PATH = "/service-path"

    def setupSpec()
    {
        LOG.info("Setting up mock hal-feed-server")
        Spark.setPort(SERVER_PORT)
        Spark.get(new Route(FEED_PATH) {
            @Override Object handle(final Request request, final Response response)
            {
                LOG.info "Making the client wait 3 SECONDS"
                SECONDS.sleep(3)
                LOG.info "Returning"
                return "Response"
            }
        })
    }

    final client = ClientBuilder.newHttpClient(1000);

    def cleanupSpec()
    {
        SparkStopper.stop()
    }

    def "should create FeedEndpoint using factory"()
    {
        when:
        def feedEndpoint = new FeedEndpointFactory(client).create("any_url")

        then:
        null != feedEndpoint
    }

    @Timeout(value = 10, unit = SECONDS)
    def 'throws SocketTimeoutException (read-timeout)'()
    {
        when:
        new FeedEndpointFactory(client).create("http://localhost:${SERVER_PORT}${FEED_PATH}").get().get()

        then:
        true
        def exception = thrown(RuntimeException)
        SocketTimeoutException.isAssignableFrom(exception.getCause().class)
    }

    @Timeout(value = 10, unit = SECONDS)
    def 'throws ConnectException'()
    {
        when:
        new FeedEndpointFactory(client).create("http://localhost:15000").get().get()

        then:
        true
        def exception = thrown(RuntimeException)
        ConnectException.isAssignableFrom(exception.getCause().class)
    }
}
