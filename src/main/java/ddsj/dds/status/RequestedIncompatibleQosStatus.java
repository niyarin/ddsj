package ddsj.dds.status;

import ddsj.dds.qos.policies.QosPolicyId;
import java.util.List;

/**
 * Status for DataReader incompatible QoS.
 *
 * @param totalCount total cumulative count
 * @param totalCountChange change since last read
 * @param lastPolicyId the last policy that was incompatible
 * @param policies counts per incompatible policy
 */
public record RequestedIncompatibleQosStatus(
        int totalCount,
        int totalCountChange,
        QosPolicyId lastPolicyId,
        List<OfferedIncompatibleQosStatus.QosPolicyCount> policies
) {
    public static final RequestedIncompatibleQosStatus INITIAL =
            new RequestedIncompatibleQosStatus(0, 0, null, List.of());
}
