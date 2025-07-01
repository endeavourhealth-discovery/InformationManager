package org.endeavourhealth.qofextractor;

import java.util.ArrayList;
import java.util.List;

public class Indicator {
    private String name;
    private String description;
    private String base;
    private List<Rule> rules = new ArrayList<>();

    public String getName() {
        return name;
    }

    public Indicator setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Indicator setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getBase() {
        return base;
    }

    public Indicator setBase(String base) {
        this.base = base;
        return this;
    }

    public List<Rule> getRules() {
        return rules;
    }

    public Indicator setRules(List<Rule> rules) {
        this.rules = rules;
        return this;
    }

    public Indicator addRule(Rule rule) {
        this.rules.add(rule);
        return this;
    }
}
