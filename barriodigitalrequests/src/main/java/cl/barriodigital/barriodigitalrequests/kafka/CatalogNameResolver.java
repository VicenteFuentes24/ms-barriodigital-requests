package cl.barriodigital.barriodigitalrequests.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "barriodigital.kafka.enabled", havingValue = "true")
public class CatalogNameResolver {
    private final HttpClient client;
    private final KafkaPublisherProperties properties;
    private final ObjectMapper mapper = new ObjectMapper();
    public CatalogNameResolver(KafkaPublisherProperties properties) {
        this.properties = properties;
        this.client = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(properties.catalogTimeoutMs())).build();
    }
    public String resolve(String procedureId) {
        final long id;
        try {
            if (!procedureId.matches("[0-9]{1,19}")) throw new IllegalArgumentException();
            id = Long.parseLong(procedureId);
            if (id <= 0) throw new IllegalArgumentException();
        } catch (IllegalArgumentException ex) { throw new NameUnavailableException("PROCEDURE_ID_NOT_CATALOG_ID"); }
        try {
            URI uri = URI.create(properties.catalogUrl().replaceAll("/+$", "") + "/api/catalog/procedures/" + id);
            var request = HttpRequest.newBuilder(uri).timeout(Duration.ofMillis(properties.catalogTimeoutMs())).GET().build();
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) throw new NameUnavailableException("CATALOG_HTTP_" + response.statusCode());
            var procedure = mapper.readValue(response.body(), Procedure.class);
            if (procedure.id() == null || procedure.id() != id || procedure.name() == null
                    || procedure.name().isBlank() || procedure.name().length() > 250) {
                throw new NameUnavailableException("CATALOG_INVALID_RESPONSE");
            }
            return procedure.name();
        } catch (NameUnavailableException ex) { throw ex;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt(); throw new NameUnavailableException("CATALOG_INTERRUPTED");
        } catch (Exception ex) { throw new NameUnavailableException("CATALOG_UNAVAILABLE"); }
    }
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Procedure(Long id, String name) { }
    public static class NameUnavailableException extends RuntimeException {
        public NameUnavailableException(String reason) { super(reason); }
    }
}
