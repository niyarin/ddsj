package ddsjdk.rtps.runtime;

import java.time.Duration;

/**
 * Status information provided when a deadline is missed.
 */
public record DeadlineMissedStatus(long totalCount, Duration deadline) {}
