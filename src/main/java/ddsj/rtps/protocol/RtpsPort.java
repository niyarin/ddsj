package ddsj.rtps.protocol;

public final class RtpsPort {
    private static final int PB = 7400;
    private static final int DG = 250;
    private static final int PG = 2;

    private RtpsPort() {
    }

    public static int metatrafficMulticast(int domainId) {
        return PB + DG * domainId;
    }

    public static int userMulticast(int domainId) {
        return PB + DG * domainId + 1;
    }

    public static int metatrafficUnicast(int domainId, int participantIndex) {
        return PB + DG * domainId + 10 + PG * participantIndex;
    }

    public static int userUnicast(int domainId, int participantIndex) {
        return PB + DG * domainId + 11 + PG * participantIndex;
    }
}
