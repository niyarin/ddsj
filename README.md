# DDSJDK

A DDS-RTPS implementation written in pure Java.

## Usage

### Publisher

Create a factory and configure the participant’s QoS, then join a DDS domain. Define a topic using a record class, create a publisher and writer, and write a message.

```java
public record Message(int id, String text) {}

// Get the factory and configure the participant.
var factory = DomainParticipantFactory.getInstance();
var qos = DomainParticipantQos.withParticipantIndex(0);

try (var participant = factory.createParticipant(0, qos)) {
    // Define a topic. Type support is derived from the record.
    var topic = participant.createTopic("HelloTopic", Message.class);

    // Create a publisher and a writer for the topic.
    var publisher = participant.createPublisher();
    var writer = publisher.createDataWriter(topic);

    // Create and publish a message.
    var message = new Message(1, "Hello DDS");
    writer.write(message);
}
```

### Subscriber

 Create a factory and configure the participant’s QoS, then join the same DDS domain. Define the topic using the publisher’s record class, create a subscriber and reader, and take incoming messages.

```java
// Get the factory and configure a separate participant.
var factory = DomainParticipantFactory.getInstance();
var qos = DomainParticipantQos.withParticipantIndex(1);

try (var participant = factory.createParticipant(0, qos)) {
  // Use the same topic name and record type as the publisher.
  var topic = participant.createTopic(
          "HelloTopic", HelloPublisher.Message.class);

  // Create a subscriber and a reader for the topic.
  var subscriber = participant.createSubscriber();
  var reader = subscriber.createDataReader(topic);

  // Receive messages and remove them from the reader's cache.
  while (!Thread.currentThread().isInterrupted()) {
      for (var sample : reader.take()) {
          if (sample.hasValidData()) {
              var message = sample.data();
              System.out.println(message);
          }
      }
      Thread.sleep(100);
  }
}
```

## Add DDSJDK to Maven or Gradle

To add a dependency using Maven:
```xml
<dependency>
    <groupId>io.github.niyarin</groupId>
    <artifactId>ddsjdk</artifactId>
    <version>0.1.0</version>
</dependency>
```

To add a dependency using Gradle:
```gradle
dependencies {
    implementation 'io.github.niyarin:ddsjdk:0.1.0'
}
```

## Test

Run tests, a specific test class.
```sh
./mvnw test
```

```sh
./mvnw '-Dtest=ReaderReceiveApiTest' test
```

Run RTPS integration test.
```sh
./mvnw -Dtest=RtpsIntegrationTest test
```
