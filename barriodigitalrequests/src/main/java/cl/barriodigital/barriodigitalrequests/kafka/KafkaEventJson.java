package cl.barriodigital.barriodigitalrequests.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.validation.Validator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "barriodigital.kafka.enabled", havingValue = "true")
public class KafkaEventJson {
    private final Validator validator;
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    public KafkaEventJson(Validator validator) { this.validator = validator; }
    public String encode(RequestKafkaEventV1 event) {
        if (!validator.validate(event).isEmpty()) throw new IllegalArgumentException("INVALID_REPORT_CONTRACT");
        try { return mapper.writeValueAsString(event); }
        catch (JsonProcessingException ex) { throw new IllegalArgumentException("JSON_ENCODING_FAILED", ex); }
    }
}
