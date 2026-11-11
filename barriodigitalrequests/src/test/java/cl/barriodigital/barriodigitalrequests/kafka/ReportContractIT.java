package cl.barriodigital.barriodigitalrequests.kafka;

import cl.barriodigital.barriodigitalrequests.model.RequestStatus;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.validation.Validation;
import java.net.URLClassLoader;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Opt-in contract test: reads actual sibling DTO sources; writes only this module's target/. */
class ReportContractIT {
    @Test void producerJsonPassesActualReportDtoConstraintsForCreationAndEveryStatus() throws Exception {
        Path sources = Path.of("../../ms-report/src/main/java/cl/barriodigital/ms_report/dto");
        Path output = Files.createDirectories(Path.of("target/report-contract-classes"));
        var compiler = ToolProvider.getSystemJavaCompiler();
        assertThat(compiler).as("Run with JDK 21, not JRE").isNotNull();
        List<String> options = new ArrayList<>(List.of("-proc:none", "--release", "21", "-encoding", "UTF-8", "-d", output.toString(),
                "-classpath", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"))));
        for (String file : List.of("RequestEventV1.java", "EventType.java", "RequestStatus.java")) {
            Path source = sources.resolve(file);
            assertThat(source).as("Actual ms-report source must exist for this opt-in profile").exists();
            options.add(source.toString());
        }
        assertThat(compiler.run(null, null, null, options.toArray(String[]::new))).isZero();
        try (var loader = new URLClassLoader(new java.net.URL[]{output.toUri().toURL()}, getClass().getClassLoader());
             var factory = Validation.buildDefaultValidatorFactory()) {
            Class<?> actualDto = loader.loadClass("cl.barriodigital.ms_report.dto.RequestEventV1");
            var mapper = JsonMapper.builder().addModule(new JavaTimeModule()).build();
            var serializer = new KafkaEventJson(factory.getValidator());
            for (var status : RequestStatus.values()) {
                boolean created = status == RequestStatus.INGRESADO;
                var event = new RequestKafkaEventV1(UUID.randomUUID(), created ? RequestKafkaEventV1.Type.REQUEST_CREATED : RequestKafkaEventV1.Type.REQUEST_STATUS_CHANGED,
                        1, 123L, created ? 1L : 2L, "12", "Permiso municipal", created ? null : RequestStatus.INGRESADO,
                        status, Instant.parse("2026-11-08T12:34:56.123456Z"));
                Object decoded = mapper.readValue(serializer.encode(event), actualDto);
                assertThat(factory.getValidator().validate(decoded)).isEmpty();
                assertThat(actualDto.getMethod("requestId").invoke(decoded).toString()).isEqualTo("123");
                assertThat(actualDto.getMethod("status").invoke(decoded).toString()).isEqualTo(status.name());
                assertThat(actualDto.getMethod("occurredAt").invoke(decoded)).isEqualTo(event.occurredAt());
            }
        }
    }
}
