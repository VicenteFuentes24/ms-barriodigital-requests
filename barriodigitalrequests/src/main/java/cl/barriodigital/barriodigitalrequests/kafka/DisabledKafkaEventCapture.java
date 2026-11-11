package cl.barriodigital.barriodigitalrequests.kafka;

import cl.barriodigital.barriodigitalrequests.model.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "barriodigital.kafka.enabled", havingValue = "false", matchIfMissing = true)
public class DisabledKafkaEventCapture implements KafkaEventCapture {
    public boolean enabled() { return false; }
    public void created(RequestEntity request) { }
    public void changed(RequestEntity request, RequestStatus previousStatus) { }
}
