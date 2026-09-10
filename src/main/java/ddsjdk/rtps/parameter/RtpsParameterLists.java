package ddsjdk.rtps.parameter;

import ddsjdk.rtps.protocol.ParameterId;

import java.util.function.Consumer;

public final class RtpsParameterLists {
    private RtpsParameterLists() {
    }

    public static byte[] payload(Consumer<RtpsParameterListWriter> writeParameters) {
        RtpsParameterListWriter writer = new RtpsParameterListWriter();
        writer.writeRaw(new byte[] {0x00, 0x03, 0x00, 0x00});
        writeParameters.accept(writer);
        writer.parameter(ParameterId.SENTINEL, new byte[0]);
        return writer.bytes();
    }
}
