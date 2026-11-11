package cl.barriodigital.barriodigitalrequests.kafka;

import java.time.Clock;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(KafkaPublisherProperties.class)
@ConditionalOnProperty(name = "barriodigital.kafka.enabled", havingValue = "true")
public class KafkaPublisherConfig {
    @Bean Clock kafkaPublisherClock() { return Clock.systemUTC(); }
    @Bean InitializingBean kafkaMigrationRequired(JdbcTemplate jdbc) {
        return () -> {
            Integer version = jdbc.queryForObject("SELECT max(version) FROM bd_requests_kafka.schema_version", Integer.class);
            if (version == null || version != 1) throw new IllegalStateException("Review/apply Kafka migration V001 before enabling publisher");
            jdbc.queryForList("SELECT request_id,last_version,tracking_status,procedure_name FROM bd_requests_kafka.stream WHERE false");
            jdbc.queryForList("SELECT event_id,payload,published_at FROM bd_requests_kafka.outbox WHERE false");
            jdbc.queryForList("SELECT event_id,previous_status,status FROM bd_requests_kafka.history_hold WHERE false");
        };
    }
}
