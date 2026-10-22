package ddsj.rtps;

import ddsj.dds.core.CdrKey;
import ddsj.dds.core.TypeSupport;
import ddsj.rtps.discovery.LocalEndpoint;
import ddsj.rtps.qos.EndpointQos;
import ddsj.rtps.runtime.RtpsParticipant;
import ddsj.rtps.runtime.RtpsDataReader;
import ddsj.rtps.runtime.ReceivedSample;
import ddsj.rtps.transport.RtpsParticipantConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import static ddsj.rtps.qos.EndpointQos.DurabilityKind.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
class KeyedDurabilityIntegrationTest {
    record Sensor(@CdrKey int id, int value) { }

    @Test void keyedHistoryAndLifecycleAcrossParticipants() throws Exception {
        var codec = TypeSupport.forCdrRecord(Sensor.class);
        for (var reliability : EndpointQos.ReliabilityKind.values()) {
            var durableQos = EndpointQos.builder().durability(TRANSIENT_LOCAL)
                    .reliability(reliability).keepLast(1).build();
            String topic = "KeyedSensors_" + reliability;
            var durable = new LocalEndpoint(topic, codec.getTypeName(), durableQos);
            var live = new LocalEndpoint(topic, codec.getTypeName(), durableQos.toBuilder().durability(VOLATILE).build());
            var writerConfig = new RtpsParticipantConfig(0, RtpsParticipantConfig.defaultMulticastGroup(), Optional.empty(), 80);
            var readerConfig = new RtpsParticipantConfig(0, RtpsParticipantConfig.defaultMulticastGroup(), Optional.empty(), 81);
            try (var sending = new RtpsParticipant(writerConfig)) {
                var writer = sending.createWriter(durable, codec);
                writer.write(new Sensor(1, 10));
                writer.write(new Sensor(2, 20));
                writer.write(new Sensor(1, 11));
                try (var receiving = new RtpsParticipant(readerConfig)) {
                    var reader = receiving.createReader(durable, codec);
                    var volatileReader = receiving.createReader(live, codec);
                    var history = new HashSet<Sensor>();
                    history.add(reader.poll(Duration.ofSeconds(5)).orElseThrow());
                    history.add(reader.poll(Duration.ofSeconds(5)).orElseThrow());
                    assertEquals(Set.of(new Sensor(1, 11), new Sensor(2, 20)), history);
                    assertTrue(volatileReader.poll(Duration.ofMillis(500)).isEmpty());
                    writer.write(new Sensor(1, 12));
                    assertEquals(new Sensor(1, 12), volatileReader.poll(Duration.ofSeconds(3)).orElseThrow());
                    assertEquals(new Sensor(1, 12), reader.poll(Duration.ofSeconds(3)).orElseThrow());
                    writer.dispose(new Sensor(1, 0));
                    var disposed = awaitSample(reader);
                    assertFalse(disposed.validData());
                    assertEquals(1, disposed.statusInfo());
                    assertEquals(1, disposed.data().id());
                    writer.unregisterInstance(new Sensor(2, 0));
                    var unregistered = awaitSample(reader);
                    assertFalse(unregistered.validData());
                    assertEquals(2, unregistered.statusInfo());
                }
            }
        }
    }

    private static <T> ReceivedSample<T> awaitSample(RtpsDataReader<T> reader) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
        while (System.nanoTime() < deadline) {
            var sample = reader.pollWithMetadata();
            if (sample.isPresent()) return sample.get();
            Thread.sleep(10);
        }
        throw new AssertionError("timed out waiting for lifecycle notification");
    }
}
