package templates;

import com.dapm.security_service.ingest_anonymization.processingStages.AnonymizationProcess;
import com.dapm.security_service.ingest_anonymization.processingStages.AttributeMappingProcess;
import com.dapm.security_service.ingest_anonymization.processingStages.EventSourceConfig;
import communication.message.impl.event.Event;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import pipeline.processingelement.Configuration;
import pipeline.processingelement.source.SimpleSource;
import reactor.util.retry.Retry;
import java.time.Duration;

public class EventSource extends SimpleSource<Event> {

    private String sseUrl;
    private AnonymizationProcess anonymizationProcess;
    private AttributeMappingProcess attributeMappingProcess;

    public EventSource(Configuration configuration) {
        super(configuration);
        try {
            sseUrl = EventSourceConfig.getEventSourceUrl(configuration);
            anonymizationProcess = AnonymizationProcess.getAnonymizationConfig(configuration);
            attributeMappingProcess = AttributeMappingProcess.getAttributeMappingConfig(configuration);
            System.out.println("✅ EventSource initialized with URL: " + sseUrl);
        } catch (Exception e) {
            System.err.println("❌ Error initializing EventSource: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to initialize EventSource", e);
        }
    }

    @Override
    protected Event process() {

        try {
            // Pull one JSON event from SSE stream
            JsonNode json = WebClient.create()
                    .get()
                    .uri(sseUrl)
                    .accept(MediaType.TEXT_EVENT_STREAM)
                    .retrieve()
                    .bodyToFlux(JsonNode.class)
                    .retryWhen(Retry.backoff(3, Duration.ofSeconds(2)))
                    .blockFirst(Duration.ofSeconds(10));

            if (json == null) {
                throw new RuntimeException("Timed out waiting for SSE event");
            }

            System.out.println("📥 Received Event: " + json);

            // Apply anonymization
            JsonNode anonymized = anonymizationProcess.apply(json);
            System.out.println("🧩 After Anonymization: " + anonymized);

            // Map to structured DAPM event
            Event dapmEvent = attributeMappingProcess.extractEvent(anonymized);

            System.out.println("✅ Event Ingested: caseId=" + dapmEvent.getCaseID()
                    + ", activity=" + dapmEvent.getActivity()
                    + ", timestamp=" + dapmEvent.getTimestamp());

            return dapmEvent;

        } catch (Exception e) {
            System.err.println("❌ Error processing event: " + e.getMessage());
            e.printStackTrace();
            return null; // allows retry
        }
    }
}
