package org.endeavourhealth.qofextractor;

public class RegisterRule {
    private int rule;
    private String logic;
    private String ifTrue;
    private String ifFalse;
    private String description;

    public int getRule() {
        return rule;
    }

    public RegisterRule setRule(int rule) {
        this.rule = rule;
        return this;
    }

    public String getLogic() {
        return logic;
    }

    public RegisterRule setLogic(String logic) {
        this.logic = logic;
        return this;
    }

    public String getIfTrue() {
        return ifTrue;
    }

    public RegisterRule setIfTrue(String ifTrue) {
        this.ifTrue = ifTrue;
        return this;
    }

    public String getIfFalse() {
        return ifFalse;
    }

    public RegisterRule setIfFalse(String ifFalse) {
        this.ifFalse = ifFalse;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public RegisterRule setDescription(String description) {
        this.description = description;
        return this;
    }
}
