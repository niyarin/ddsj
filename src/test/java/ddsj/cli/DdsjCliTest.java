package ddsj.cli;

import ddsj.rtps.discovery.RemotePublication;
import ddsj.rtps.qos.EndpointQos;
import ddsj.rtps.types.EntityId;
import ddsj.rtps.types.GuidPrefix;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DdsjCliTest {
    @Test void helpDoesNotOpenNetwork() {
        for (String[] args : List.of(new String[0], new String[]{"--help"},
                new String[]{"discover", "--help"})) {
            var out = new ByteArrayOutputStream();
            var err = new ByteArrayOutputStream();
            assertEquals(0, DdsjCli.run(args, new PrintStream(out), new PrintStream(err)));
            assertTrue(out.toString().contains("Usage:"));
            assertEquals("", err.toString());
        }
    }

    @Test void invalidArgumentsFailBeforeDiscovery() {
        for (String[] args : List.of(new String[]{"unknown"},
                new String[]{"discover", "--unknown"},
                new String[]{"discover", "--wait"},
                new String[]{"discover", "--wait", "-1"},
                new String[]{"discover", "--wait", "NaN"},
                new String[]{"discover", "--domain", "233"},
                new String[]{"discover", "--domain", "232", "--participant-index", "119"},
                new String[]{"discover", "--participant-index", "120"})) {
            var out = new ByteArrayOutputStream();
            var err = new ByteArrayOutputStream();
            assertEquals(2, DdsjCli.run(args, new PrintStream(out), new PrintStream(err)));
            assertEquals("", out.toString());
            assertTrue(err.toString().startsWith("ddsj:"));
        }
    }

    @Test void outputUsesHexGuidAndEscapesRemoteControlCharacters() {
        var endpoint = new RemotePublication(new GuidPrefix(new byte[12])
                .toGuid(new EntityId(new byte[]{0, 0, 1, 3})),
                "hello\nworld", "Type\tName", EndpointQos.DEFAULT);
        var out = new ByteArrayOutputStream();
        DdsjCli.printEndpoints("Writers", List.of(endpoint), new PrintStream(out));
        assertTrue(out.toString().contains("Writers (1)"));
        assertTrue(out.toString().contains("00000000000000000000000000000103\thello\\u000aworld\tType\\u0009Name"));
    }

    @Test void emptyDiscoveryIsExplicit() {
        var out = new ByteArrayOutputStream();
        DdsjCli.printEndpoints("Readers", List.of(), new PrintStream(out));
        assertTrue(out.toString().contains("Readers (0)"));
        assertTrue(out.toString().contains("No endpoints discovered."));
    }
}
