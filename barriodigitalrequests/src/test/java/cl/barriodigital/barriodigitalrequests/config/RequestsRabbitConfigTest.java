package cl.barriodigital.barriodigitalrequests.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

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
    @Test
    void topologyPreservesExistingNamesTypesAndRoutes() {
        var topology = new RequestsRabbitConfig().requestCommandTopology();
        assertThat(topology.getDeclarablesByType(Exchange.class))
                .extracting(Exchange::getName, Exchange::getType)
                .containsExactlyInAnyOrder(tuple("cmd.direct", "direct"), tuple("cmd.topic", "topic"),
                        tuple("cmd.dead.dlx", "direct"));
        assertThat(topology.getDeclarablesByType(Queue.class)).extracting(Queue::getName)
                .containsExactlyInAnyOrder("q.cmd.email", "q.cmd.crew", "q.cmd.certificate",
                        "q.cmd.email.dlq", "q.cmd.crew.dlq", "q.cmd.certificate.dlq");
        assertThat(topology.getDeclarablesByType(Binding.class))
                .extracting(Binding::getDestination, Binding::getExchange, Binding::getRoutingKey)
                .containsExactlyInAnyOrder(
                        tuple("q.cmd.email", "cmd.direct", "email.send"),
                        tuple("q.cmd.crew", "cmd.direct", "crew.ticket"),
                        tuple("q.cmd.certificate", "cmd.direct", "certificate.gen"),
                        tuple("q.cmd.email", "cmd.topic", "email.*"),
                        tuple("q.cmd.crew", "cmd.topic", "crew.#"),
                        tuple("q.cmd.certificate", "cmd.topic", "certificate.*"),
                        tuple("q.cmd.email.dlq", "cmd.dead.dlx", "q.cmd.email.dlq"),
                        tuple("q.cmd.crew.dlq", "cmd.dead.dlx", "q.cmd.crew.dlq"),
                        tuple("q.cmd.certificate.dlq", "cmd.dead.dlx", "q.cmd.certificate.dlq"));
        assertThat(topology.getDeclarablesByType(Binding.class)).allSatisfy(binding -> {
            assertThat(binding.getDestinationType()).isEqualTo(Binding.DestinationType.QUEUE);
            assertThat(binding.getArguments()).isEmpty();
        });
    }

}
