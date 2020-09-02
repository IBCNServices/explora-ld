package util;

import model.Aggregate;
import model.TSFragmentData;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TSFragmentsAggregator implements Consumer<List<LinkedHashMap<String, Object>>> {
    List<LinkedHashMap<String, Object>> aggregateList = new ArrayList<>();

    public List<LinkedHashMap<String, Object>> getAggregateList() {
        return aggregateList;
    }

    @Override
    public void accept(List<LinkedHashMap<String, Object>> tsFragmentData) {
        aggregateList.addAll(tsFragmentData);
    }

    public void combine(TSFragmentsAggregator other) {
        aggregateList.addAll(other.aggregateList);
    }

}
