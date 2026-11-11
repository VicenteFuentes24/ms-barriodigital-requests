package cl.barriodigital.barriodigitalrequests.kafka;

import jakarta.validation.constraints.*;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "barriodigital.kafka")
public record KafkaPublisherProperties(
        boolean enabled,
        @NotBlank @Pattern(regexp = "bd\\.requests\\.events\\.v1") String topic,
        @NotBlank String catalogUrl,
        @Min(100) @Max(10000) int catalogTimeoutMs,
        @Min(1) @Max(100) int batchSize,
        @Min(1000) @Max(60000) long sendTimeoutMs,
        @Min(100) long retryInitialMs,
        @Min(100) long retryMaxMs) {
    public long retryDelay(int attempts) {
        long factor = 1L << Math.min(Math.max(attempts - 1, 0), 20);
        return retryInitialMs > retryMaxMs / factor ? retryMaxMs : Math.min(retryMaxMs, retryInitialMs * factor);
    }
}
