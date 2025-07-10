package org.endeavourhealth.qofextractor;

public class ExtractionField {
    private int field;
    private String name;
    private String cluster;
    private String logic;
    private String description;

    public int getField() {
        return field;
    }

    public ExtractionField setField(int field) {
        this.field = field;
        return this;
    }

    public String getName() {
        return name;
    }

    public ExtractionField setName(String name) {
        this.name = name;
        return this;
    }

    public String getCluster() {
        return cluster;
    }

    public ExtractionField setCluster(String cluster) {
        this.cluster = cluster;
        return this;
    }

    public String getLogic() {
        return logic;
    }

    public ExtractionField setLogic(String logic) {
        this.logic = logic;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ExtractionField setDescription(String description) {
        this.description = description;
        return this;
    }
}
