package ddsjdk.dds.core;

import ddsjdk.dds.exception.DdsException;
import ddsjdk.dds.exception.ReturnCode;
import ddsjdk.dds.listener.DomainParticipantListener;
import ddsjdk.dds.qos.DomainParticipantQos;
import ddsjdk.dds.status.StatusMask;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Factory for creating DomainParticipants.
 * <p>
 * DomainParticipantFactory is a singleton that manages the lifecycle of
 * all DomainParticipants in the process.
 */
public final class DomainParticipantFactory {
    private static final DomainParticipantFactory INSTANCE = new DomainParticipantFactory();

    private final Map<Integer, List<DomainParticipant>> participantsByDomain = new ConcurrentHashMap<>();
    private volatile DomainParticipantQos defaultQos = DomainParticipantQos.DEFAULT;

    private DomainParticipantFactory() {}

    /**
     * Returns the singleton factory instance.
     *
     * @return the factory
     */
    public static DomainParticipantFactory getInstance() {
        return INSTANCE;
    }

    /**
     * Creates a DomainParticipant with default QoS.
     *
     * @param domainId the domain ID
     * @return the created participant
     */
    public DomainParticipant createParticipant(int domainId) {
        return createParticipant(domainId, defaultQos, null, StatusMask.NONE);
    }

    /**
     * Creates a DomainParticipant with specified QoS.
     *
     * @param domainId the domain ID
     * @param qos the QoS policies
     * @return the created participant
     */
    public DomainParticipant createParticipant(int domainId, DomainParticipantQos qos) {
        return createParticipant(domainId, qos, null, StatusMask.NONE);
    }

    /**
     * Creates a DomainParticipant with specified QoS and listener.
     *
     * @param domainId the domain ID
     * @param qos the QoS policies
     * @param listener the listener
     * @param mask the status mask
     * @return the created participant
     */
    public DomainParticipant createParticipant(int domainId, DomainParticipantQos qos,
                                                DomainParticipantListener listener, StatusMask mask) {
        try {
            DomainParticipant participant = new DomainParticipant(domainId, qos);
            participant.setListener(listener, mask);
            participantsByDomain.computeIfAbsent(domainId, k -> new CopyOnWriteArrayList<>())
                    .add(participant);
            return participant;
        } catch (IOException e) {
            throw new DdsException(ReturnCode.ERROR, "Failed to create participant", e);
        }
    }

    /**
     * Deletes a DomainParticipant.
     *
     * @param participant the participant to delete
     * @return OK if successful
     */
    public ReturnCode deleteParticipant(DomainParticipant participant) {
        participant.close();
        return ReturnCode.OK;
    }

    /**
     * Looks up a participant by domain ID.
     *
     * @param domainId the domain ID
     * @return the first participant in the domain, or empty
     */
    public Optional<DomainParticipant> lookupParticipant(int domainId) {
        List<DomainParticipant> list = participantsByDomain.get(domainId);
        if (list == null || list.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(list.get(0));
    }

    /**
     * Returns the default participant QoS.
     *
     * @return the default QoS
     */
    public DomainParticipantQos getDefaultParticipantQos() {
        return defaultQos;
    }

    /**
     * Sets the default participant QoS.
     *
     * @param qos the default QoS
     */
    public void setDefaultParticipantQos(DomainParticipantQos qos) {
        this.defaultQos = qos != null ? qos : DomainParticipantQos.DEFAULT;
    }

    /**
     * Called internally when a participant is closed.
     */
    void removeParticipant(DomainParticipant participant) {
        List<DomainParticipant> list = participantsByDomain.get(participant.getDomainId());
        if (list != null) {
            list.remove(participant);
        }
    }
}
