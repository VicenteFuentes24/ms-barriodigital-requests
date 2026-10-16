package cl.barriodigital.barriodigitalrequests.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.mock.env.MockEnvironment;

class RabbitAddressConfigurationTest {
    @Test
    void localHostAndPortRemainTheDefaults() throws IOException {
        var environment = configuration();
        assertThat(environment.getProperty("spring.rabbitmq.addresses")).isEmpty();
        assertThat(environment.getProperty("spring.rabbitmq.host")).isEqualTo("localhost");
        assertThat(environment.getProperty("spring.rabbitmq.port")).isEqualTo("5672");
    }

    @Test
    void acceptsMultiplePrivateAddressesWithoutChangingPublisherGuarantees() throws IOException {
        var environment = configuration().withProperty("RABBITMQ_ADDRESSES", "10.0.1.62:5672,10.0.2.151:5672");
        assertThat(environment.getProperty("spring.rabbitmq.addresses")).isEqualTo("10.0.1.62:5672,10.0.2.151:5672");
        assertThat(environment.getProperty("spring.rabbitmq.publisher-confirm-type")).isEqualTo("correlated");
        assertThat(environment.getProperty("spring.rabbitmq.publisher-returns")).isEqualTo("true");
        assertThat(environment.getProperty("spring.rabbitmq.template.mandatory")).isEqualTo("true");
        assertThat(environment.getProperty("spring.rabbitmq.dynamic")).isEqualTo("false");
    }

    @Test
    void customSingleHostIsStillConfigurable() throws IOException {
        var environment = configuration().withProperty("RABBITMQ_HOST", "rabbitmq").withProperty("RABBITMQ_PORT", "5673");
        assertThat(environment.getProperty("spring.rabbitmq.addresses")).isEmpty();
        assertThat(environment.getProperty("spring.rabbitmq.host")).isEqualTo("rabbitmq");
        assertThat(environment.getProperty("spring.rabbitmq.port")).isEqualTo("5673");
    }

    private MockEnvironment configuration() throws IOException {
        var properties = new Properties();
        try (var input = getClass().getResourceAsStream("/application.properties")) {
            properties.load(input);
        }
        var environment = new MockEnvironment();
        environment.getPropertySources().addLast(new PropertiesPropertySource("application", properties));
        return environment;
    }
}
