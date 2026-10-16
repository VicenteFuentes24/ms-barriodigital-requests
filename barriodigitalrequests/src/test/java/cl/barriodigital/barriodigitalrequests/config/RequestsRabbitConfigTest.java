package cl.barriodigital.barriodigitalrequests.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;

class RequestsRabbitConfigTest {

    @Test
    void topologyIsDurableButNeverDeclaredByRequestsAndCannotOverrideDlxPolicies() {
        var topology = new RequestsRabbitConfig().requestCommandTopology();
        assertThat(topology.getDeclarables()).hasSize(18).allSatisfy(declarable ->
                assertThat(declarable.shouldDeclare()).isFalse());
        assertThat(topology.getDeclarablesByType(Exchange.class)).hasSize(3).allSatisfy(exchange -> {
            assertThat(exchange.isDurable()).isTrue();
            assertThat(exchange.isAutoDelete()).isFalse();
        });
        assertThat(topology.getDeclarablesByType(Queue.class)).hasSize(6).allSatisfy(queue -> {
            assertThat(queue.isDurable()).isTrue();
            assertThat(queue.isExclusive()).isFalse();
            assertThat(queue.isAutoDelete()).isFalse();
            assertThat(queue.getArguments()).isEqualTo(Map.of("x-queue-type", "classic"));
        });
        assertThat(topology.getDeclarablesByType(Binding.class)).hasSize(9);
    }
}
