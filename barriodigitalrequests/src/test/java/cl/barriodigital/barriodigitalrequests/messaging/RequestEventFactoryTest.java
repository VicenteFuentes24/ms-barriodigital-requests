package cl.barriodigital.barriodigitalrequests.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import cl.barriodigital.barriodigitalrequests.dto.RequestEvent;
import cl.barriodigital.barriodigitalrequests.model.RequestEntity;
import cl.barriodigital.barriodigitalrequests.model.RequestStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

class RequestEventFactoryTest {

    @Test
    void malformedTraceMetadataIsReplacedAndTheEventIsAnImmutableSnapshot() {
        MockHttpServletRequest http = new MockHttpServletRequest();
        http.addHeader("X-Trace-Id", "trace\nunsafe");
        http.addHeader("X-Correlation-Id", "x".repeat(129));
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(http));
        try {
            RequestEntity entity = new RequestEntity();
            entity.setStatus(RequestStatus.INGRESADO);
            RequestEvent event = new RequestEventFactory().create(RequestEvent.Type.REQUEST_CREATED, entity);
            entity.setStatus(RequestStatus.ADMITIDO);
            assertThat(UUID.fromString(event.traceId())).isNotNull();
            assertThat(event.correlationId()).isEqualTo(event.traceId());
            assertThat(event.status()).isEqualTo(RequestStatus.INGRESADO);
        } finally {
            RequestContextHolder.resetRequestAttributes();
        }
    }

    @Test
    void usesAnExistingMdcTraceWhenAvailable() {
        MDC.put("traceId", "abc123");
        try {
            assertThat(new RequestEventFactory().create(RequestEvent.Type.REQUEST_CREATED, new RequestEntity()).traceId())
                    .isEqualTo("abc123");
        } finally {
            MDC.remove("traceId");
        }
    }
}
