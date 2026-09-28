package ddsj.cli;

import ddsj.dds.core.DomainParticipantFactory;
import ddsj.dds.qos.DomainParticipantQos;
import ddsj.rtps.discovery.RemoteEndpoint;
import ddsj.rtps.protocol.RtpsPort;

import java.io.PrintStream;
import java.net.NetworkInterface;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;

/** Command-line endpoint discovery. */
public final class DdsjCli {
    private DdsjCli() {}

    private static final String HELP = """
            Usage: java -jar <ddsj.jar> discover [options]
              --domain N             DDS domain ID, 0-232 (default: 0)
              --wait N               Discovery wait in whole seconds (default: 5)
              --participant-index N  Local participant index, 0-119 (default: 0)
              --interface NAME       Network interface (default: system selection)
              --help, -h             Show this help
            Lists remote writers and readers with QoS; discovery does not imply matching.
            Use a distinct participant index for each process on the same host/domain.
            """;

    /** Runs the CLI, returning process status 0, 1 (runtime failure), or 2 (usage error). */
    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    static int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length == 0 || (args.length == 1 && isHelp(args[0]))) {
            out.print(HELP);
            return 0;
        }
        int domain = 0;
        int seconds = 5;
        int index = 0;
        String interfaceName = null;
        try {
            if (!args[0].equals("discover")) {
                throw new IllegalArgumentException("Unknown command: " + args[0]);
            }
            for (int i = 1; i < args.length; i++) {
                String option = args[i];
                if (isHelp(option)) {
                    out.print(HELP);
                    return 0;
                }
                if (!List.of("--domain", "--wait", "--participant-index", "--interface").contains(option)) {
                    throw new IllegalArgumentException("Unknown option: " + option);
                }
                if (++i == args.length) throw new IllegalArgumentException("Missing value for " + option);
                String value = args[i];
                switch (option) {
                    case "--domain" -> domain = number(option, value, 232);
                    case "--wait" -> seconds = number(option, value, Integer.MAX_VALUE);
                    case "--participant-index" -> index = number(option, value, 119);
                    case "--interface" -> interfaceName = value;
                    default -> throw new AssertionError(option);
                }
            }
            if (RtpsPort.userUnicast(domain, index) > 65535) {
                throw new IllegalArgumentException("Domain and participant index exceed the UDP port range");
            }
        } catch (IllegalArgumentException e) {
            err.println("ddsj: " + e.getMessage());
            err.print(HELP);
            return 2;
        }
        try {
            var qos = DomainParticipantQos.builder().participantIndex(index);
            if (interfaceName != null) {
                var network = NetworkInterface.getByName(interfaceName);
                if (network == null) {
                    err.println("ddsj: Unknown network interface: " + interfaceName);
                    return 2;
                }
                qos.networkInterface(network);
            }
            try (var participant = DomainParticipantFactory.getInstance().createParticipant(domain, qos.build())) {
                err.printf("Discovering endpoints in domain %d for %d seconds...%n", domain, seconds);
                Thread.sleep(seconds * 1000L);
                printEndpoints("Writers", participant.getDiscoveredPublications(), out);
                printEndpoints("Readers", participant.getDiscoveredSubscriptions(), out);
            }
            return 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            err.println("ddsj: Discovery interrupted");
            return 1;
        } catch (Exception e) {
            Throwable cause = e;
            while (cause.getCause() != null) cause = cause.getCause();
            err.println("ddsj: " + e.getMessage()
                    + (cause == e ? "" : ": " + cause.getMessage()));
            return 1;
        }
    }

    private static boolean isHelp(String value) {
        return value.equals("--help") || value.equals("-h");
    }

    private static int number(String option, String value, int maximum) {
        try {
            int result = Integer.parseInt(value);
            if (result >= 0 && result <= maximum) return result;
        } catch (NumberFormatException ignored) {
            // Report the option and its accepted range below.
        }
        throw new IllegalArgumentException(option + " must be an integer from 0 to " + maximum);
    }

    static void printEndpoints(String label, List<? extends RemoteEndpoint> endpoints, PrintStream out) {
        out.printf("%s (%d)%n", label, endpoints.size());
        if (endpoints.isEmpty()) {
            out.println("  No endpoints discovered.");
            return;
        }
        out.println("GUID\tTOPIC\tTYPE\tQOS");
        endpoints.stream().sorted(Comparator.comparing(endpoint ->
                HexFormat.of().formatHex(endpoint.endpointGuid().bytes())))
                .forEach(endpoint -> out.printf("%s\t%s\t%s\t%s%n",
                        HexFormat.of().formatHex(endpoint.endpointGuid().bytes()),
                        escape(endpoint.topicName()), escape(endpoint.typeName()), endpoint.qos()));
    }

    private static String escape(String value) {
        StringBuilder result = new StringBuilder();
        value.codePoints().forEach(c -> {
            if (Character.isISOControl(c)) result.append(String.format("\\u%04x", c));
            else if (c == '\\') result.append("\\\\");
            else result.appendCodePoint(c);
        });
        return result.toString();
    }
}
