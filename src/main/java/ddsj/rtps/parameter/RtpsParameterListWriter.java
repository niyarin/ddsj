package ddsj.rtps.parameter;

import ddsj.rtps.util.RtpsIo;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public final class RtpsParameterListWriter {
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    public byte[] bytes() {
        return out.toByteArray();
    }

    public void writeRaw(byte[] bytes) {
        out.writeBytes(bytes);
    }

    public void parameter(int id, byte[] value) {
        out.write(id & 0xff);
        out.write((id >>> 8) & 0xff);
        out.write(value.length & 0xff);
        out.write((value.length >>> 8) & 0xff);
        out.writeBytes(value);
        align(4);
    }

    public void stringParameter(int id, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream valueOut = new ByteArrayOutputStream();
        valueOut.writeBytes(RtpsIo.intLe(bytes.length + 1));
        valueOut.writeBytes(bytes);
        valueOut.write(0);
        while (valueOut.size() % 4 != 0) {
            valueOut.write(0);
        }
        parameter(id, valueOut.toByteArray());
    }

    public void int32Parameter(int id, int value) {
        parameter(id, RtpsIo.intLe(value));
    }

    private void align(int boundary) {
        while (out.size() % boundary != 0) {
            out.write(0);
        }
    }
}
