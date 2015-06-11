package com.qmetric.feed.consumer

import org.glassfish.jersey.client.ClientProperties
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spark.Request
import spark.Response
import spark.Route
import spark.Spark
import spark.SparkStopper
import spock.lang.Specification
import spock.lang.Timeout

import javax.ws.rs.ProcessingException
import javax.ws.rs.client.Client
import javax.ws.rs.client.ClientBuilder

import static java.util.concurrent.TimeUnit.SECONDS

class FeedEndpointFactoryTest extends Specification {

    private static final Logger LOG = LoggerFactory.getLogger(FeedEndpointFactoryTest)
    static timeout = new FeedEndpointFactory.ConnectionTimeout(SECONDS, 1)
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
        1 * client.property(ClientProperties.CONNECT_TIMEOUT, _ as Integer)
        1 * client.property(ClientProperties.READ_TIMEOUT, _ as Integer)
        null != feedEndpoint
    }

    @Timeout(value = 10, unit = SECONDS) def 'throws SocketTimeoutException (read-timeout)'()
    {
        when:
        new FeedEndpointFactory(ClientBuilder.newClient(), timeout).create("http://localhost:${SERVER_PORT}${FEED_PATH}").get().get()

        then:
        def exception = thrown(ProcessingException)
        SocketTimeoutException.isAssignableFrom(exception.getCause().class)
    }

    @Timeout(value = 10, unit = SECONDS) def 'throws ConnectException'()
    {
        when:
        new FeedEndpointFactory(ClientBuilder.newClient(), timeout).create("http://localhost:15000").get().get()

        then:
        def exception = thrown(ProcessingException)
        ConnectException.isAssignableFrom(exception.getCause().class)
    }
}
