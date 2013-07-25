HAL+JSON feed consumer
======================

[![Build Status](https://travis-ci.org/qmetric/hal-feed-consumer.png)](https://travis-ci.org/qmetric/hal-feed-consumer)

Java library used to consume [HAL+JSON](http://stateless.co/hal_specification.html) feeds produced by [hal-feed-server](https://github.com/qmetric/hal-feed-server).

Features
---------

* Guaranteed execution of feed entries in ascending publish date order
* Supports multiple consumers - refer to [Competing consumer pattern](#competing-consumer-pattern)
* Built-in health checks and metrics provided


Usage
-----

First, configure a data store used by the consumer to track which feed entries have already been consumed.
An [Amazon SimpleDB](http://aws.amazon.com/simpledb/) based implementation is supplied as part of this library:

```java
final AmazonSimpleDB simpleDBClient = new AmazonSimpleDBClient(new BasicAWSCredentials("access key", "secret key"));
simpleDBClient.setRegion(getRegion(EU_WEST_1));

final ConsumedStore consumedStore = new SimpleDBConsumedStore(simpleDBClient, "your-sdb-domain");
```

Then, create and start a feed consumer:

```java
final FeedConsumerConfiguration feedConsumer = new FeedConsumerConfiguration()
                .fromUrl("http://your-feed-endpoint")
                .withConsumedStore(consumedStore)
                .consumeEachEntryWith(new ConsumeAction() {
                                          @Override public void consume(final ReadableRepresentation feedEntry) {
                                              System.out.println("write your code here to consume the next feed entry...");
                                      }})
                .pollForNewEntriesEvery(5, MINUTES)
                .start();
```

Library available from [Maven central repository](http://search.maven.org/)

```
<dependency>
    <groupId>com.qmetric</groupId>
    <artifactId>hal-feed-consumer</artifactId>
    <version>${VERSION}</version>
</dependency>
```


Health checks and metrics
-------------------------

Built-in health checks and metrics are available by default using [codahale metrics](http://metrics.codahale.com/):

Codahale metric and health check registries can be retrieved from your feed consumer configuration class:

```java
final HealthCheckRegistry healthCheckRegistry = feedConsumer.getHealthCheckRegistry();

final MetricRegistry metricRegistry = feedConsumer.getMetricRegistry();
```


Competing consumer pattern
--------------------------

Supports the competing consumer pattern. Multiple consumers can read from the same feed and be configured with the same ConsumedStore.

To guarantee feed entries are consumed in ascending publish date order. This implementation ensures only a single consumer can consume at any one time - concurrent processing of
feed entries is avoided to preserve the guaranteed execution ordering. A consumer attempting to consume an entry that is already being consumed by another consumer, will be
delayed until the other consumer has finished consuming that entry.

The main advantage for configuring multiple consumers is for failover. Increase to throughput is likely to be minimal, although feed polling rates may be more frequent.
