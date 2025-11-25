package templates;

import communication.message.Message;
import communication.message.impl.event.Event;
import pipeline.processingelement.Configuration;
import pipeline.processingelement.operator.SimpleOperator;

import java.util.HashMap;
import java.util.Map;

public class DepartmentFilter extends SimpleOperator<Event> {
    private final String department;
    // Only count received events per second
    private long receivedCount = 0;
    private long lastReportTime = System.currentTimeMillis();


    public DepartmentFilter(Configuration configuration) {
        super(configuration);
        // Read department from configuration
        department = configuration.get("department").toString();
    }

    @Override
    protected Event process(Message message, int portNumber) {

        receivedCount++;

        long now = System.currentTimeMillis();
        if (now - lastReportTime >= 1000) {
            System.out.println("[DepartmentFilter] Events received in last second: " + receivedCount);

            receivedCount = 0;
            lastReportTime = now;
        }

        Event event = (Event) message;

        // Debug print incoming events
//        System.out.println("[DepartmentFilter] Incoming event: "
//                + event.getCaseID() + " - " + event.getActivity()
//                + " in " + event.getAttributes());

        boolean matches = event.getAttributes().stream()
                .anyMatch(attribute ->
                        attribute.getName().equals("department") &&
                                attribute.getValue().toString().equalsIgnoreCase(department));

        if (matches) {
//            System.out.println("[DepartmentFilter] ✅ Forwarding event for department: " + department);
            return event;
        } else {
//            System.out.println("[DepartmentFilter] ❌ Dropping event (not " + department + ")");
            return null;
        }
    }

    @Override
    protected Map<Class<? extends Message>, Integer> setConsumedInputs() {
        Map<Class<? extends Message>, Integer> map = new HashMap<>();
        map.put(Event.class, 1);
        return map;
    }
}
