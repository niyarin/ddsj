package ddsjdk.rtps.discovery;

import ddsjdk.rtps.types.GuidPrefix;
import ddsjdk.rtps.types.Locator;

import java.time.Duration;
import java.util.Set;

public record RemoteParticipant(
        GuidPrefix guidPrefix,
        Set<Locator> metatrafficUnicast,
        Set<Locator> userUnicast,
        Duration leaseDuration) {
    public static final Duration DEFAULT_PARTICIPANT_LEASE_DURATION = Duration.ofSeconds(30);

    public RemoteParticipant {
        metatrafficUnicast = Set.copyOf(metatrafficUnicast);
        userUnicast = Set.copyOf(userUnicast);
    }

    public RemoteParticipant(GuidPrefix guidPrefix, Set<Locator> metatrafficUnicast, Set<Locator> userUnicast) {
        this(guidPrefix, metatrafficUnicast, userUnicast, DEFAULT_PARTICIPANT_LEASE_DURATION);
    }
}
