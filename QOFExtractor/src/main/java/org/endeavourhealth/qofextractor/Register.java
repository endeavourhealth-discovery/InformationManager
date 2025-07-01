package org.endeavourhealth.qofextractor;

import java.util.ArrayList;
import java.util.List;

public class Register {
    private String name;
    private String description;
    private String base;
    private List<RegisterRule> rules = new ArrayList<>();

    public String getName() {
        return name;
    }

    public Register setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Register setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getBase() {
        return base;
    }

    public Register setBase(String base) {
        this.base = base;
        return this;
    }

    public List<RegisterRule> getRules() {
        return rules;
    }

    public Register setRules(List<RegisterRule> rules) {
        this.rules = rules;
        return this;
    }

    public Register addRule(RegisterRule rule) {
        this.rules.add(rule);
        return this;
    }
}
