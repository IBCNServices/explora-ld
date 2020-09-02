package model;

import java.util.LinkedHashMap;
import java.util.TreeMap;

public class TSFragmentData {
    public TreeMap<Long, LinkedHashMap<String, Object>> graph;

    public TSFragmentData() {
    }

    public TSFragmentData(TreeMap<Long, LinkedHashMap<String, Object>> graph) {
        this.graph = graph;
    }

    @Override
    public String toString() {
        return "TSFragmentData{" +
                "graph=" + graph +
                '}';
    }
}
