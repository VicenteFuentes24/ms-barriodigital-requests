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

    @Bean
    public Jackson2JsonMessageConverter requestEventMessageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }

    @Bean
    public Declarables requestCommandTopology() {
        List<AbstractDeclarable> topology = new ArrayList<>();
        topology.add(new DirectExchange(COMMAND_EXCHANGE, true, false));
        topology.add(new TopicExchange("cmd.topic", true, false));
        topology.add(new DirectExchange("cmd.dead.dlx", true, false));
        for (String kind : List.of("email", "crew", "certificate")) {
            String queue = "q.cmd." + kind;
            topology.add(new Queue(queue, true, false, false, Map.of("x-queue-type", "classic")));
            topology.add(new Queue(queue + ".dlq", true, false, false, Map.of("x-queue-type", "classic")));
            topology.add(binding(queue + ".dlq", "cmd.dead.dlx", queue + ".dlq"));
        }
        topology.add(binding("q.cmd.email", COMMAND_EXCHANGE, EMAIL_KEY));
        topology.add(binding("q.cmd.crew", COMMAND_EXCHANGE, CREW_KEY));
        topology.add(binding("q.cmd.certificate", COMMAND_EXCHANGE, CERTIFICATE_KEY));
        topology.add(binding("q.cmd.email", "cmd.topic", "email.*"));
        topology.add(binding("q.cmd.crew", "cmd.topic", "crew.#"));
        topology.add(binding("q.cmd.certificate", "cmd.topic", "certificate.*"));

        // Infrastructure owns declarations and DLX policies; even a later RabbitAdmin must not redeclare these.
        topology.forEach(declarable -> declarable.setShouldDeclare(false));
        return new Declarables(topology);
    }

    private Binding binding(String queue, String exchange, String key) {
        return new Binding(queue, Binding.DestinationType.QUEUE, exchange, key, Map.of());
    }
}
