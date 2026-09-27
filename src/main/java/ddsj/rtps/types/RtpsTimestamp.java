package ddsj.rtps.types;

import java.time.Instant;

public record RtpsTimestamp(int seconds, int fraction) {
    public static final RtpsTimestamp INVALID = new RtpsTimestamp(0xffffffff, 0xffffffff);

    public static RtpsTimestamp now() {
        Instant now = Instant.now();
        long epochSeconds = now.getEpochSecond();
        int nanos = now.getNano();
        int fraction = (int) ((nanos / 1_000_000_000.0) * 0xffffffffL);
        return new RtpsTimestamp((int) epochSeconds, fraction);
    }

    public static RtpsTimestamp fromInstant(Instant instant) {
        long epochSeconds = instant.getEpochSecond();
        int nanos = instant.getNano();
        int fraction = (int) ((nanos / 1_000_000_000.0) * 0xffffffffL);
        return new RtpsTimestamp((int) epochSeconds, fraction);
    }

    public Instant toInstant() {
        if (this.equals(INVALID)) {
            return null;
        }
        long nanos = (long) ((fraction & 0xffffffffL) / (double) 0xffffffffL * 1_000_000_000L);
        return Instant.ofEpochSecond(seconds & 0xffffffffL, nanos);
    }

    public boolean isValid() {
        return !this.equals(INVALID);
    }
}
