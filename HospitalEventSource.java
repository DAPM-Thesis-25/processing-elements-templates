package templates;

import communication.message.impl.event.Attribute;
import communication.message.impl.event.Event;
import org.springframework.stereotype.Component;
import pipeline.processingelement.Configuration;
import pipeline.processingelement.source.WebSource;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.*;

/**
 * High-throughput HospitalEventSource
 * ----------------------------------
 * Emits realistic hospital workflow events that can be mined
 * into a meaningful Petri net by the Heuristic Miner.
 *
 * Emergency department always follows the same sequential flow:
 *   ADMISSION → TRIAGE → DIAGNOSIS → TREATMENT → DISCHARGE
 *
 * Other departments (Cardiology, Neurology, Oncology, Pediatrics)
 * follow variable process variants.
 */
@Component
public class HospitalEventSource extends WebSource<Event> {

    private static final Random RANDOM = new Random();

    private static final List<String> DEPARTMENTS =
            Arrays.asList("Emergency", "Cardiology", "Neurology", "Oncology", "Pediatrics");

    // Fixed flow for Emergency
    private static final List<String> EMERGENCY_FLOW = Arrays.asList(
            "ADMISSION", "TRIAGE", "DIAGNOSIS", "TREATMENT", "DISCHARGE"
    );

    // Variable flows for other departments
    private static final List<List<String>> PROCESS_VARIANTS = Arrays.asList(
            Arrays.asList("ADMISSION", "DIAGNOSIS", "LAB_TEST", "TREATMENT", "DISCHARGE"),
            Arrays.asList("ADMISSION", "TRIAGE", "DIAGNOSIS", "DISCHARGE"),
            Arrays.asList("ADMISSION", "TRIAGE", "DIAGNOSIS", "TREATMENT", "TREATMENT", "DISCHARGE")
    );

    // Active patient cases (caseID → iterator of events)
    private final Map<String, Iterator<String>> activeCases = new HashMap<>();

    // Keep track of department per case
    private final Map<String, String> caseDepartments = new HashMap<>();

    // Adjust throughput freely
    private static final int EVENTS_PER_SECOND = 100000;

    public HospitalEventSource(Configuration configuration) {
        super(configuration);
    }

    @Override
    protected Flux<Event> process() {
        return Flux.interval(Duration.ofMillis(1000L / EVENTS_PER_SECOND))
                .map(tick -> generateNextEvent())
                .filter(Objects::nonNull)
                .doOnNext(e -> {
                    //System.out.println("🏥 " + e)
                        }
                )
                .onErrorContinue(
                        (err, obj) ->
                        {
                            //System.err.println("Error emitting hospital event: " + err);
                        }
                );
    }

    private Event generateNextEvent() {
        // Occasionally start new patient cases
        if (activeCases.size() < 100 && RANDOM.nextDouble() < 0.4) {
            startNewCase();
        }

        if (activeCases.isEmpty()) return null;

        // Pick a random active patient and advance their process
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

        // Randomly assign department
        String department = DEPARTMENTS.get(RANDOM.nextInt(DEPARTMENTS.size()));

        // Emergency always follows fixed flow
        List<String> variant;
        if (department.equals("Emergency")) {
            variant = EMERGENCY_FLOW;
        } else {
            variant = PROCESS_VARIANTS.get(RANDOM.nextInt(PROCESS_VARIANTS.size()));
        }

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
