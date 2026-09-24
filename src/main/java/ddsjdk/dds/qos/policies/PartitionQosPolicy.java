package ddsjdk.dds.qos.policies;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Partition QoS policy for Publisher and Subscriber.
 * <p>
 * Partitions provide a logical isolation mechanism within a domain.
 * Publishers and Subscribers only communicate if their partition sets
 * have at least one matching partition name.
 * <p>
 * Partition names support wildcard matching using '*' and '?'.
 *
 * @param names the partition names
 */
public record PartitionQosPolicy(List<String> names) {

    /** Default partition (empty string matches only other empty partitions) */
    public static final PartitionQosPolicy DEFAULT = new PartitionQosPolicy(List.of(""));

    public PartitionQosPolicy {
        Objects.requireNonNull(names, "names");
        names = List.copyOf(names);
        for (String name : names) {
            if (name == null) {
                throw new IllegalArgumentException("partition names must not contain null");
            }
        }
    }

    /**
     * Creates a partition policy with a single partition name.
     *
     * @param name the partition name
     * @return a partition policy
     */
    public static PartitionQosPolicy of(String name) {
        return new PartitionQosPolicy(List.of(Objects.requireNonNull(name, "name")));
    }

    /**
     * Creates a partition policy with multiple partition names.
     *
     * @param names the partition names
     * @return a partition policy
     */
    public static PartitionQosPolicy of(String... names) {
        return new PartitionQosPolicy(List.of(names));
    }

    /**
     * Creates a partition policy with multiple partition names.
     *
     * @param names the partition names
     * @return a partition policy
     */
    public static PartitionQosPolicy of(List<String> names) {
        return new PartitionQosPolicy(names);
    }

    /**
     * Checks if this partition matches another partition.
     * <p>
     * Two partitions match if at least one name from each set matches.
     * Names can contain wildcards: '*' matches any sequence, '?' matches any single character.
     *
     * @param other the other partition policy
     * @return true if the partitions match
     */
    public boolean matches(PartitionQosPolicy other) {
        for (String thisName : names) {
            for (String otherName : other.names) {
                if (partitionMatches(thisName, otherName)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean partitionMatches(String pattern1, String pattern2) {
        // If neither has wildcards, simple equality
        if (!hasWildcard(pattern1) && !hasWildcard(pattern2)) {
            return pattern1.equals(pattern2);
        }
        // If one has wildcards, use it as the pattern
        if (hasWildcard(pattern1)) {
            return matchesWildcard(pattern1, pattern2);
        }
        return matchesWildcard(pattern2, pattern1);
    }

    private static boolean hasWildcard(String s) {
        return s.contains("*") || s.contains("?");
    }

    private static boolean matchesWildcard(String pattern, String text) {
        String regex = wildcardToRegex(pattern);
        return Pattern.matches(regex, text);
    }

    private static String wildcardToRegex(String wildcard) {
        StringBuilder sb = new StringBuilder();
        for (char c : wildcard.toCharArray()) {
            switch (c) {
                case '*' -> sb.append(".*");
                case '?' -> sb.append(".");
                case '.' -> sb.append("\\.");
                case '\\' -> sb.append("\\\\");
                case '[', ']', '{', '}', '(', ')', '^', '$', '|', '+' -> {
                    sb.append("\\");
                    sb.append(c);
                }
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }
}
