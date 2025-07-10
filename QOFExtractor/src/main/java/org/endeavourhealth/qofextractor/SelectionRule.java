package org.endeavourhealth.qofextractor;

public class SelectionRule {
    private String logic;
    private String ifTrue;
    private String ifFalse;
    private String description;

    public String getLogic() {
        return logic;
    }

    public SelectionRule setLogic(String logic) {
        this.logic = logic;
        return this;
    }

    public String getIfTrue() {
        return ifTrue;
    }

    public SelectionRule setIfTrue(String ifTrue) {
        this.ifTrue = ifTrue;
        return this;
    }

    public String getIfFalse() {
        return ifFalse;
    }

    public SelectionRule setIfFalse(String ifFalse) {
        this.ifFalse = ifFalse;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public SelectionRule setDescription(String description) {
        this.description = description;
        return this;
    }
}
