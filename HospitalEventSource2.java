package templates;

import com.dapm.security_service.ingest_anonymization.processingStages.AnonymizationProcess;
import com.dapm.security_service.ingest_anonymization.processingStages.AttributeMappingProcess;
import communication.message.impl.event.Attribute;
import communication.message.impl.event.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import pipeline.processingelement.Configuration;
import pipeline.processingelement.source.SimpleSource;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.*;

/**
 * HospitalEventSource2
 * --------------------
 * Generates random hospital workflow events locally (no SSE),
 * applies anonymization, and emits clean DAPM events.
 */
public class HospitalEventSource2 extends SimpleSource<Event> {

    private static final Random RANDOM = new Random();

    private static final List<String> DEPARTMENTS = Arrays.asList(
            "Emergency", "Cardiology", "Neurology", "Oncology", "Pediatrics"
    );

    private static final List<String> EMERGENCY_FLOW = Arrays.asList(
            "ADMISSION", "TRIAGE", "DIAGNOSIS", "TREATMENT", "DISCHARGE"
    );

    private static final List<List<String>> PROCESS_VARIANTS = Arrays.asList(
            Arrays.asList("ADMISSION", "DIAGNOSIS", "LAB_TEST", "TREATMENT", "DISCHARGE"),
            Arrays.asList("ADMISSION", "TRIAGE", "DIAGNOSIS", "DISCHARGE"),
            Arrays.asList("ADMISSION", "TRIAGE", "DIAGNOSIS", "TREATMENT", "TREATMENT", "DISCHARGE")
    );

    private final Map<String, Iterator<String>> activeCases = new HashMap<>();
    private final Map<String, String> caseDepartments = new HashMap<>();

    private final AnonymizationProcess anonymizationProcess;
    private final AttributeMappingProcess attributeMappingProcess;

    private static final int EVENTS_PER_SECOND = 200;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public HospitalEventSource2(Configuration configuration) {
        super(configuration);

        try {
            System.out.println("⚙️ Initializing HospitalEventSource2 (random event generator)");

            anonymizationProcess = AnonymizationProcess.getAnonymizationConfig(configuration);
            attributeMappingProcess = AttributeMappingProcess.getAttributeMappingConfig(configuration);

            System.out.println("✅ HospitalEventSource2 initialized successfully.");

        } catch (Exception e) {
            System.err.println("❌ Error initializing HospitalEventSource2: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to initialize HospitalEventSource2", e);
        }
    }

    @Override
    protected Event process() {
        try {
            // Generate a random event
            Event event = generateNextEvent();
            if (event == null) return null;

            // Convert Event → JsonNode
            Map<String, Object> jsonMap = new LinkedHashMap<>();
            jsonMap.put("patientId", event.getCaseID());
            jsonMap.put("activity", event.getActivity());
            jsonMap.put("timestamp", event.getTimestamp());
            for (Attribute<?> attr : event.getAttributes()) {
                jsonMap.put(attr.getName(), attr.getValue());
            }
            JsonNode json = MAPPER.valueToTree(jsonMap);

            // Apply anonymization
            json = anonymizationProcess.apply(json);
            System.out.println("🧩 After Anonymization: " + json);

            // Extract structured DAPM Event
            Event dapmEvent = attributeMappingProcess.extractEvent(json);

            System.out.println("✅ Event Ingested: "
                    + "caseId=" + dapmEvent.getCaseID()
                    + ", activity=" + dapmEvent.getActivity()
                    + ", timestamp=" + dapmEvent.getTimestamp());

            for (Attribute<?> attr : dapmEvent.getAttributes()) {
                System.out.println("   ↳ " + attr.getName() + " = " + attr.getValue());
            }



            return dapmEvent;

        } catch (Exception e) {
            System.err.println("❌ Error generating hospital event: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    // Optional: Continuous stream (for testing)
    public Flux<Event> streamEvents() {
        return Flux.interval(Duration.ofMillis(1000L / EVENTS_PER_SECOND))
                .map(tick -> process())
                .filter(Objects::nonNull)
                .doOnNext(e -> System.out.println("🏥 " + e))
                .onErrorContinue((err, obj) ->
                        System.err.println("Error emitting hospital event: " + err));
    }

    private Event generateNextEvent() {
        if (activeCases.size() < 100 && RANDOM.nextDouble() < 0.4) {
            startNewCase();
        }

        if (activeCases.isEmpty()) return null;

        String caseId = getRandomKey(activeCases);
        Iterator<String> steps = activeCases.get(caseId);

        if (!steps.hasNext()) {
            activeCases.remove(caseId);
            caseDepartments.remove(caseId);
            return null;
        }

        String activity = steps.next();
        String department = caseDepartments.get(caseId);
        String timestamp = String.valueOf(System.currentTimeMillis());

        Set<Attribute<?>> attributes = new HashSet<>();
        attributes.add(new Attribute<>("department", department));
        attributes.add(new Attribute<>("doctor", "Dr." + (char) ('A' + RANDOM.nextInt(26))));
        attributes.add(new Attribute<>("severity", RANDOM.nextInt(5) + 1));

        return new Event(caseId, activity, timestamp, attributes);
    }

    private void startNewCase() {
        String newCaseId = "PAT-" + (1000 + RANDOM.nextInt(9000));
        String department = DEPARTMENTS.get(RANDOM.nextInt(DEPARTMENTS.size()));
        List<String> variant = department.equals("Emergency")
                ? EMERGENCY_FLOW
                : PROCESS_VARIANTS.get(RANDOM.nextInt(PROCESS_VARIANTS.size()));

        activeCases.put(newCaseId, variant.iterator());
        caseDepartments.put(newCaseId, department);
    }

    private static <K> K getRandomKey(Map<K, ?> map) {
        int index = RANDOM.nextInt(map.size());
        Iterator<K> iter = map.keySet().iterator();
        for (int i = 0; i < index; i++) iter.next();
        return iter.next();
    }
}
