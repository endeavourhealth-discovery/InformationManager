package org.endeavourhealth.qofextractor;

public class Rule {
    private int rule;
    private String logic;
    private String ifTrue;
    private String ifFalse;
    private String description;

    public int getRule() {
        return rule;
    }

    public Rule setRule(int rule) {
        this.rule = rule;
        return this;
    }

    public String getLogic() {
        return logic;
    }

    public Rule setLogic(String logic) {
        this.logic = logic;
        return this;
    }

    public String getIfTrue() {
        return ifTrue;
    }

    public Rule setIfTrue(String ifTrue) {
        this.ifTrue = ifTrue;
        return this;
    }

    public String getIfFalse() {
        return ifFalse;
    }

    public Rule setIfFalse(String ifFalse) {
        this.ifFalse = ifFalse;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Rule setDescription(String description) {
        this.description = description;
        return this;
    }
}
