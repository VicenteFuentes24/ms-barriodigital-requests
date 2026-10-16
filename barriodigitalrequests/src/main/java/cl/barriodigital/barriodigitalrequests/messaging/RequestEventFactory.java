package cl.barriodigital.barriodigitalrequests.messaging;

import java.time.Instant;
import java.util.UUID;
import java.util.regex.Pattern;

import cl.barriodigital.barriodigitalrequests.dto.RequestEvent;
import cl.barriodigital.barriodigitalrequests.model.RequestEntity;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@Component
public class RequestEventFactory {

    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[A-Za-z0-9._-]{1,128}");

    public RequestEvent create(RequestEvent.Type type, RequestEntity request) {
        String traceId = validOrDefault(MDC.get("traceId"),
                validOrDefault(header("X-Trace-Id"), UUID.randomUUID().toString()));
        String correlationId = validOrDefault(header("X-Correlation-Id"), traceId);
        return new RequestEvent(type, UUID.randomUUID().toString(), Instant.now(), traceId, correlationId,
                request.getId(), request.getProcedureTypeId(), request.getStatus(), request.getCreatedBy());
    }

    private String header(String name) {
        if (RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes attributes) {
            return attributes.getRequest().getHeader(name);
        }
        return null;
    }

    private String validOrDefault(String value, String fallback) {
        return value != null && SAFE_IDENTIFIER.matcher(value).matches() ? value : fallback;
    }
}
