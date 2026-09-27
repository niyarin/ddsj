package ddsjdk.dds.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies a fixed-length array in CDR serialization.
 * <p>
 * Use on record components of type byte[] to indicate a fixed-length
 * array instead of a variable-length sequence.
 * <p>
 * Example:
 * <pre>
 * record Message(
 *     {@literal @}CdrFixedLength(16) byte[] uuid,  // fixed 16 bytes
 *     byte[] data                                   // variable length sequence
 * ) {}
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.RECORD_COMPONENT)
public @interface CdrFixedLength {
    /**
     * The fixed length of the array.
     *
     * @return the array length
     */
    int value();
}
