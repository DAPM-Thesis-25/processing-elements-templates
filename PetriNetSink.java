package templates;

import communication.message.Message;
import communication.message.impl.petrinet.PetriNet;
import communication.message.impl.petrinet.Place;
import communication.message.impl.petrinet.Transition;
import communication.message.impl.petrinet.arc.Arc;

import communication.message.impl.petrinet.arc.PlaceToTransitionArc;
import communication.message.impl.petrinet.arc.TransitionToPlaceArc;
import guru.nidi.graphviz.attribute.Color;
import guru.nidi.graphviz.attribute.Shape;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.model.MutableNode;
import pipeline.processingelement.Configuration;
import pipeline.processingelement.Sink;
import utils.Pair;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static guru.nidi.graphviz.engine.Format.SVG;
import static guru.nidi.graphviz.engine.Graphviz.fromGraph;
import static guru.nidi.graphviz.model.Factory.*;

public class PetriNetSink extends Sink {
    private String latestSvg = "";
    public PetriNetSink(Configuration configuration) {
        super(configuration);
    }

    public String getLatestSvg() {
        return latestSvg;
    }

    @Override
    public void observe(Pair<Message, Integer> messageAndPortNumber) {
        PetriNet petriNet = (PetriNet) messageAndPortNumber.first();
        //System.out.println("\n===== New PetriNet Snapshot =====");
        //System.out.println("Places: " + petriNet.getPlaces().size());
        //System.out.println("Transitions: " + petriNet.getTransitions().size());
        //System.out.println("Arcs: " + petriNet.getFlowRelation().size());

        // Print each place
        //petriNet.getPlaces().forEach(p -> System.out.println("Place: " + p.getID()));

        // Print each transition
        //petriNet.getTransitions().forEach(t -> System.out.println("Transition: " + t.getID()));

        // Print each arc with source and target
        for (Arc arc : petriNet.getFlowRelation()) {
            if (arc instanceof PlaceToTransitionArc) {
                PlaceToTransitionArc p2t = (PlaceToTransitionArc) arc;
                //System.out.println("[P→T] " + p2t.getSource().getID() + " -> " + p2t.getTarget().getID());
            } else if (arc instanceof TransitionToPlaceArc) {
                TransitionToPlaceArc t2p = (TransitionToPlaceArc) arc;
                //System.out.println("[T→P] " + t2p.getSource().getID() + " -> " + t2p.getTarget().getID());
            } else {
                //System.out.println("[Unknown Arc] " + arc.getID());
            }
        }

        try {
            MutableGraph dotGraph = constructDotGraph(petriNet);
            //System.out.println("DOT Graph Source:\n" + dotGraph.toString());

            //String svg = fromGraph(dotGraph).width(1024).render(SVG).toString();

            this.latestSvg = dotGraph.toString(); // ✅ store in the instance
            //System.out.println("PetriNetSink updated SVG length=" + svg.length());

//            fromGraph(dotGraph)
//                    .render(SVG)
//                    .toFile(new File("orgA/src/main/resources/sinks/outputs/petriNet.svg"));
        } catch (Exception e) {
            throw new RuntimeException("Failed to render PetriNet", e);
        }
    }

    @Override
    protected Map<Class<? extends Message>, Integer> setConsumedInputs() {
        Map<Class<? extends Message>, Integer> map = new HashMap<>();
        map.put(PetriNet.class, 1);
        return map;
    }

    private MutableGraph constructDotGraph(PetriNet petriNet) {
        MutableGraph dotGraph = mutGraph("petriNet").setDirected(true);

        Set<Place> places = petriNet.getPlaces();
        Set<Transition> transitions = petriNet.getTransitions();
        Set<Arc> arcs = petriNet.getFlowRelation();

        Set<PlaceToTransitionArc> placeToTransitionArcs = arcs.stream()
                .filter(arc -> arc instanceof PlaceToTransitionArc)
                .map(arc -> (PlaceToTransitionArc) arc)
                .collect(Collectors.toSet());

        Set<TransitionToPlaceArc> transitionToPlaceArcs = arcs.stream()
                .filter(arc -> arc instanceof TransitionToPlaceArc)
                .map(arc -> (TransitionToPlaceArc) arc)
                .collect(Collectors.toSet());


        Map<String, MutableNode> nodeMap = new HashMap<>();

        for (Place p : places) {
            MutableNode placeNode = mutNode(p.getID()).add(Shape.CIRCLE, Color.GREEN);
            dotGraph.add(placeNode);
            nodeMap.put(p.getID(), placeNode);
        }

        for (Transition t : transitions) {
            MutableNode transNode = mutNode(t.getID()).add(Shape.BOX, Color.BLUE);
            dotGraph.add(transNode);
            nodeMap.put(t.getID(), transNode);
        }

        for (PlaceToTransitionArc a : placeToTransitionArcs) {
            String src = a.getSource().getID();
            String tgt = a.getTarget().getID();
            if(nodeMap.containsKey(src) && nodeMap.containsKey(tgt)) {
                nodeMap.get(src).addLink(nodeMap.get(tgt));
            }
        }

        for (TransitionToPlaceArc a : transitionToPlaceArcs) {
            String src = a.getSource().getID();
            String tgt = a.getTarget().getID();
            if(nodeMap.containsKey(src) && nodeMap.containsKey(tgt)) {
                nodeMap.get(src).addLink(nodeMap.get(tgt));
            }
        }
        return dotGraph;
    }
}