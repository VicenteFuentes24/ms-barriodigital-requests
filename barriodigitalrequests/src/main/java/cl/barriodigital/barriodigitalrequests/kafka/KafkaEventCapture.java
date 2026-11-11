package cl.barriodigital.barriodigitalrequests.kafka;

import cl.barriodigital.barriodigitalrequests.model.*;

/** Capture participates in RequestService's transaction; no network I/O. */
public interface KafkaEventCapture {
    boolean enabled();
    void created(RequestEntity request);
    void changed(RequestEntity request, RequestStatus previousStatus);
}
