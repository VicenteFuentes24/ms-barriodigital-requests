package cl.barriodigital.barriodigitalrequests.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.AbstractDeclarable;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class RequestsRabbitConfig {

    public static final String COMMAND_EXCHANGE = "cmd.direct";
    public static final String EMAIL_KEY = "email.send";
    public static final String CREW_KEY = "crew.ticket";
    public static final String CERTIFICATE_KEY = "certificate.gen";

    public static final String TOPIC_EXCHANGE = "cmd.topic";
    public static final String DEAD_LETTER_EXCHANGE = "cmd.dead.dlx";
    public static final String EMAIL_QUEUE = "q.cmd.email";
    public static final String CREW_QUEUE = "q.cmd.crew";
    public static final String CERTIFICATE_QUEUE = "q.cmd.certificate";
    public static final String EMAIL_DLQ = EMAIL_QUEUE + ".dlq";
    public static final String CREW_DLQ = CREW_QUEUE + ".dlq";
    public static final String CERTIFICATE_DLQ = CERTIFICATE_QUEUE + ".dlq";
    public static final String EMAIL_PATTERN = "email.*";
    public static final String CREW_PATTERN = "crew.#";
    public static final String CERTIFICATE_PATTERN = "certificate.*";

    private static final Map<String, Object> QUEUE_ARGUMENTS = Map.of("x-queue-type", "classic");

    @Bean
    public Jackson2JsonMessageConverter requestEventMessageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }

    @Bean
    public Declarables requestCommandTopology() {
        List<AbstractDeclarable> topology = new ArrayList<>();
        topology.add(new DirectExchange(COMMAND_EXCHANGE, true, false));
        topology.add(new TopicExchange(TOPIC_EXCHANGE, true, false));
        topology.add(new DirectExchange(DEAD_LETTER_EXCHANGE, true, false));
        for (String queue : List.of(EMAIL_QUEUE, EMAIL_DLQ, CREW_QUEUE, CREW_DLQ,
                CERTIFICATE_QUEUE, CERTIFICATE_DLQ)) {
            topology.add(new Queue(queue, true, false, false, QUEUE_ARGUMENTS));
        }
        for (String dlq : List.of(EMAIL_DLQ, CREW_DLQ, CERTIFICATE_DLQ)) {
            topology.add(binding(dlq, DEAD_LETTER_EXCHANGE, dlq));
        }
        topology.add(binding(EMAIL_QUEUE, COMMAND_EXCHANGE, EMAIL_KEY));
        topology.add(binding(CREW_QUEUE, COMMAND_EXCHANGE, CREW_KEY));
        topology.add(binding(CERTIFICATE_QUEUE, COMMAND_EXCHANGE, CERTIFICATE_KEY));
        topology.add(binding(EMAIL_QUEUE, TOPIC_EXCHANGE, EMAIL_PATTERN));
        topology.add(binding(CREW_QUEUE, TOPIC_EXCHANGE, CREW_PATTERN));
        topology.add(binding(CERTIFICATE_QUEUE, TOPIC_EXCHANGE, CERTIFICATE_PATTERN));

        // Infrastructure owns declarations and DLX policies; even a later RabbitAdmin must not redeclare these.
        topology.forEach(declarable -> declarable.setShouldDeclare(false));
        return new Declarables(topology);
    }

    private Binding binding(String queue, String exchange, String key) {
        return new Binding(queue, Binding.DestinationType.QUEUE, exchange, key, Map.of());
    }
}
