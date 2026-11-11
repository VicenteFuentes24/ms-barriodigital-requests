package cl.barriodigital.barriodigitalrequests.kafka;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class CatalogNameResolverTest {
    @Test void readsRealCatalogHttpContractAndRejectsWrongIdentifiers() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/api/catalog/procedures/12", exchange -> {
            byte[] body = "{\"id\":12,\"name\":\"Licencia municipal\",\"available\":false,\"requirements\":[]}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200,body.length); exchange.getResponseBody().write(body); exchange.close();
        });
        server.start();
        try {
            var resolver = new CatalogNameResolver(KafkaEventJsonTest.properties("http://127.0.0.1:"+server.getAddress().getPort(),1000));
            assertThat(resolver.resolve("12")).isEqualTo("Licencia municipal");
            assertThatThrownBy(() -> resolver.resolve("procedure-1")).hasMessage("PROCEDURE_ID_NOT_CATALOG_ID");
            assertThatThrownBy(() -> resolver.resolve("13")).hasMessage("CATALOG_HTTP_404");
        } finally { server.stop(0); }
    }
    @Test void refusesANameForADifferentCatalogId() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/", exchange -> {
            byte[] body = "{\"id\":99,\"name\":\"Different procedure\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200,body.length); exchange.getResponseBody().write(body); exchange.close();
        });
        server.start();
        try {
            var resolver = new CatalogNameResolver(KafkaEventJsonTest.properties("http://127.0.0.1:"+server.getAddress().getPort(),1000));
            assertThatThrownBy(() -> resolver.resolve("12")).hasMessage("CATALOG_INVALID_RESPONSE");
        } finally { server.stop(0); }
    }
}
