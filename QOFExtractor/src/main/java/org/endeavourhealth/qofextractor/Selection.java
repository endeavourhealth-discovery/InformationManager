package org.endeavourhealth.qofextractor;

import java.util.ArrayList;
import java.util.List;

public class Selection {
    private String name;
    private List<SelectionRule> rules = new ArrayList<>();

    public String getName() {
        return name;
    }

    public Selection setName(String name) {
        this.name = name;
        return this;
    }

    public List<SelectionRule> getRules() {
        return rules;
    }

    public Selection setRules(List<SelectionRule> rules) {
        this.rules = rules;
        return this;
    }

    public Selection addRule(SelectionRule rule) {
        this.rules.add(rule);
        return this;
    }
}
