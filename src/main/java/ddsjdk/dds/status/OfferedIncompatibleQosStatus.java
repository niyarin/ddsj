package ddsjdk.dds.status;

import ddsjdk.dds.qos.policies.*;
import java.util.List;

/**
 * Status for DataWriter incompatible QoS.
 *
 * @param totalCount total cumulative count
 * @param totalCountChange change since last read
 * @param lastPolicyId the last policy that was incompatible
 * @param policies counts per incompatible policy
 */
public record OfferedIncompatibleQosStatus(
        int totalCount,
        int totalCountChange,
        QosPolicyId lastPolicyId,
        List<QosPolicyCount> policies
) {
    public static final OfferedIncompatibleQosStatus INITIAL =
            new OfferedIncompatibleQosStatus(0, 0, null, List.of());

    public record QosPolicyCount(QosPolicyId policyId, int count) {}
}
