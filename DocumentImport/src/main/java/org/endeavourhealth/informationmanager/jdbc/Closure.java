package org.endeavourhealth.informationmanager.jdbc;

public class Closure {
    private Integer parent;
    private Integer level;

    public Integer getParent() {
        return parent;
    }

    public Closure setParent(Integer parent) {
        this.parent = parent;
        return this;
    }

    public Integer getLevel() {
        return level;
    }

    public Closure setLevel(Integer level) {
        this.level = level;
        return this;
    }


}
