package org.endeavourhealth.informationmanager.common.transform.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PropertyAxiom extends Axiom {
    private String property;

    @JsonProperty("Property")
    public String getProperty() {
        return property;
    }

    public PropertyAxiom setProperty(String property) {
        this.property = property;
        return this;
    }
}
