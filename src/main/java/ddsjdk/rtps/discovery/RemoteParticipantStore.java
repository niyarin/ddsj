package ddsjdk.rtps.discovery;

import ddsjdk.rtps.types.GuidPrefix;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class RemoteParticipantStore implements Iterable<RemoteParticipant> {
    private final ConcurrentHashMap<GuidPrefix, TimedRemoteParticipant> participantsByPrefix = new ConcurrentHashMap<>();

    public void upsert(RemoteParticipant participant) {
        upsert(participant, System.nanoTime());
    }

    public void upsert(RemoteParticipant participant, long nowNanos) {
        participantsByPrefix.put(participant.guidPrefix(), new TimedRemoteParticipant(participant, nowNanos));
    }

    public Optional<RemoteParticipant> get(GuidPrefix guidPrefix) {
        TimedRemoteParticipant participant = participantsByPrefix.get(guidPrefix);
        return participant == null ? Optional.empty() : Optional.of(participant.participant());
    }

    public Optional<RemoteParticipant> remove(GuidPrefix guidPrefix) {
        TimedRemoteParticipant participant = participantsByPrefix.remove(guidPrefix);
        return participant == null ? Optional.empty() : Optional.of(participant.participant());
    }

    public List<RemoteParticipant> removeExpired() {
        return removeExpired(System.nanoTime());
    }

    public List<RemoteParticipant> removeExpired(long nowNanos) {
        List<RemoteParticipant> expired = new ArrayList<>();
        participantsByPrefix.entrySet().removeIf(entry -> {
            long leaseNanos = entry.getValue().participant().leaseDuration().toNanos();
            boolean isExpired = leaseNanos > 0 && nowNanos - entry.getValue().lastSeenNanos() > leaseNanos;
            if (isExpired) {
                expired.add(entry.getValue().participant());
            }
            return isExpired;
        });
        return expired;
    }

    public List<RemoteParticipant> snapshot() {
        return participantsByPrefix.values().stream().map(TimedRemoteParticipant::participant).toList();
    }

    @Override
    public Iterator<RemoteParticipant> iterator() {
        return snapshot().iterator();
    }

    private record TimedRemoteParticipant(RemoteParticipant participant, long lastSeenNanos) {
    }
}
