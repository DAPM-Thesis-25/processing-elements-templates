package templates;

import communication.message.Message;
import communication.message.impl.event.Event;
import pipeline.processingelement.Configuration;
import pipeline.processingelement.operator.SimpleOperator;

import java.util.HashMap;
import java.util.Map;

public class LanguageFilter extends SimpleOperator<Event> {
    private final String language;

    public LanguageFilter(Configuration configuration) {
        super(configuration);
        language = configuration.get("language").toString().toLowerCase();
    }

    @Override
    protected Event process(Message message, int portNumber) {
        Event event = (Event) message;
        if(event.getAttributes().stream()
                .anyMatch(attribute -> attribute.getName()
                        .equals("domain") && attribute.getValue()
                        .toString().contains(language + "."))) {
            return event;
        }
        return null;
    }

    @Override
    protected Map<Class<? extends Message>, Integer> setConsumedInputs() {
        Map<Class<? extends Message>, Integer> map = new HashMap<>();
        map.put(Event.class, 1);
        return map;
    }
}
