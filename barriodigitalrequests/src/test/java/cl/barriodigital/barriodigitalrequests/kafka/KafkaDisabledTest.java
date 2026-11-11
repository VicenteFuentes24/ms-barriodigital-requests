package cl.barriodigital.barriodigitalrequests.kafka;

import cl.barriodigital.barriodigitalrequests.dto.*;
import cl.barriodigital.barriodigitalrequests.messaging.RequestEventPublisher;
import cl.barriodigital.barriodigitalrequests.model.RequestStatus;
import cl.barriodigital.barriodigitalrequests.service.RequestService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.assertj.core.api.Assertions.*;

@SpringBootTest(properties={"spring.config.import=","barriodigital.kafka.enabled=false",
        "spring.datasource.url=jdbc:h2:mem:kafka-disabled;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.driver-class-name=org.h2.Driver","spring.datasource.username=sa","spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop","spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
        "logging.level.root=WARN","debug=false"})
class KafkaDisabledTest {
    @Autowired RequestService requests;
    @Autowired ApplicationContext context;
    @Autowired JdbcTemplate jdbc;
    @MockBean RequestEventPublisher rabbit;
    @Test void oldSchemaWorksWithoutAnyKafkaTablesOrWorker() {
        var created = requests.createRequest(new CreateRequestRequest("procedure-1","description"),"owner@example.invalid");
        var updated = requests.updateStatus(created.id(),new UpdateRequestStatusRequest(RequestStatus.ADMITIDO,null));
        assertThat(updated.status()).isEqualTo(RequestStatus.ADMITIDO);
        assertThat(updated.createdBy()).isEqualTo("owner@example.invalid");
        assertThat(context.getBeansOfType(KafkaOutboxWorker.class)).isEmpty();
        assertThat(context.getBeansOfType(TransactionalKafkaEventCapture.class)).isEmpty();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM information_schema.tables WHERE lower(table_schema)='bd_requests_kafka'",Long.class)).isZero();
    }
}
