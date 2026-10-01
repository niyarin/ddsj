package ddsj.cdr;

import ddsj.dds.core.CdrRecordTypeSupport;
import ddsj.dds.core.TypeSupport;
import org.junit.jupiter.api.Test;
import java.util.HexFormat;
import static org.junit.jupiter.api.Assertions.*;

class CdrCodecTest {
    static final class Position {
        final int x;
        final long y;
        Position(int x, long y) { this.x = x; this.y = y; }
    }
    static final class Message {
        final byte tag;
        final Position position;
        final int[] samples;
        Message(byte tag, Position position, int[] samples) {
            this.tag = tag; this.position = position; this.samples = samples;
        }
    }
    record RecordPosition(int x, long y) {}
    record RecordMessage(byte tag, RecordPosition position, int[] samples) {}

    private static final CdrCodec<Position> POSITION = new CdrCodec<>() {
        public void write(CdrWriter out, Position value) {
            out.writeInt(value.x);
            out.writeLong(value.y);
        }
        public Position read(CdrReader in) { return new Position(in.readInt(), in.readLong()); }
    };
    private static final CdrCodec<int[]> SAMPLES = CdrCodecs.sequence(int[].class, CdrCodecs.INT);
    private static final CdrCodec<Message> MESSAGE = new CdrCodec<>() {
        public void write(CdrWriter out, Message value) {
            out.writeByte(value.tag);
            POSITION.write(out, value.position);
            SAMPLES.write(out, value.samples);
        }
        public Message read(CdrReader in) {
            return new Message(in.readByte(), POSITION.read(in), SAMPLES.read(in));
        }
    };

    @Test void ordinaryClassAndRecordUseIdenticalBytesAndDecodeEachOther() {
        var automatic = CdrRecordTypeSupport.of(RecordMessage.class);
        var record = new RecordMessage((byte) 0x11, new RecordPosition(2, 3), new int[]{4, 5});
        var plain = new Message(record.tag(), new Position(2, 3), record.samples());
        for (CdrEncoding encoding : CdrEncoding.values()) {
            var serializer = new CdrPayloadSerializer<>(MESSAGE, encoding);
            var support = TypeSupport.fromSerializer(Message.class, "example::Message", serializer);
            byte[] expected = encoding == CdrEncoding.XCDR1 ? automatic.serialize(record) : automatic.serializeCdr2(record);
            assertArrayEquals(expected, support.serialize(plain));
            Message decoded = support.deserialize(expected);
            assertEquals(plain.tag, decoded.tag);
            assertEquals(plain.position.x, decoded.position.x);
            assertEquals(plain.position.y, decoded.position.y);
            assertArrayEquals(plain.samples, decoded.samples);
            assertArrayEquals(record.samples(), automatic.deserialize(support.serialize(plain)).samples());
            assertEquals("example::Message", support.getTypeName());
            assertEquals(Message.class, support.getType());
        }
    }

    @Test void arrayLimitsAreConfigurableAndCumulative() {
        var codec = new CdrCodec<int[][]>() {
            public void write(CdrWriter out, int[][] arrays) {
                SAMPLES.write(out, arrays[0]);
                SAMPLES.write(out, arrays[1]);
            }
            public int[][] read(CdrReader in) { return new int[][]{SAMPLES.read(in), SAMPLES.read(in)}; }
        };
        var limited = new CdrPayloadSerializer<>(codec, CdrEncoding.XCDR1, 3);
        byte[] bytes = limited.serialize(new int[][]{{1, 2}, {3, 4}});
        assertThrows(IllegalArgumentException.class, () -> limited.deserialize(bytes));
        var allowed = new CdrPayloadSerializer<>(codec, CdrEncoding.XCDR1, 4);
        assertArrayEquals(new int[]{3, 4}, allowed.deserialize(bytes)[1]);
        assertThrows(IllegalArgumentException.class, () -> new CdrPayloadSerializer<>(codec, CdrEncoding.XCDR1, -1));
    }

    @Test void fixedArrayAndSequenceFactoriesRejectBadDefinitionsAndValues() {
        assertThrows(IllegalArgumentException.class, () -> CdrCodecs.sequence(String.class, CdrCodecs.STRING));
        assertThrows(IllegalArgumentException.class, () -> CdrCodecs.sequence(int[][].class, SAMPLES));
        assertThrows(IllegalArgumentException.class, () -> CdrCodecs.fixedArray(int[].class, CdrCodecs.INT, 0));
        var fixed = new CdrPayloadSerializer<>(CdrCodecs.fixedArray(int[].class, CdrCodecs.INT, 2));
        assertThrows(IllegalArgumentException.class, () -> fixed.serialize(new int[]{1}));
        assertThrows(NullPointerException.class, () -> fixed.serialize(null));
    }

    @Test void standaloneHandwrittenCodecHasKnownWireBytes() {
        var support = new CdrPayloadSerializer<>(MESSAGE);
        byte[] expected = HexFormat.of().parseHex(
                "0001000011000000020000000300000000000000020000000400000005000000");
        assertArrayEquals(expected, support.serialize(new Message((byte) 0x11, new Position(2, 3), new int[]{4, 5})));
        assertArrayEquals(new int[]{4, 5}, support.deserialize(expected).samples);
    }
}
