package model;

import java.io.Serializable;
import java.util.List;

public class Message implements Serializable {
    private final List<String> columns;
    private final List data;
    private final Object metadata;

    public Message(List<String> columns, List data, Object metadata) {
        this.columns = columns;
        this.data = data;
        this.metadata = metadata;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List getData() {
        return data;
    }

    public Object getMetadata() {
        return metadata;
    }
}
