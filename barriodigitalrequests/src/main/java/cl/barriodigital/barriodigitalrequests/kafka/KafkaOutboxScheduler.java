package cl.barriodigital.barriodigitalrequests.kafka;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = {"barriodigital.kafka.enabled", "barriodigital.kafka.worker-enabled"}, havingValue = "true")
public class KafkaOutboxScheduler {
    private final KafkaOutboxWorker worker;
    private final KafkaPublisherProperties properties;
    public KafkaOutboxScheduler(KafkaOutboxWorker worker, KafkaPublisherProperties properties) {
        this.worker = worker; this.properties = properties;
    }
    @Scheduled(fixedDelayString = "${barriodigital.kafka.poll-ms}")
    public void tick() {
        for (int n=0; n<properties.batchSize() && !Thread.currentThread().isInterrupted(); n++) {
            // Calls cross the transactional proxy: prepare commits before publish begins.
            boolean prepared = worker.prepareNext();
            boolean published = worker.publishNext();
            if (!prepared && !published) break;
        }
    }
}
