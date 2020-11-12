package org.endeavourhealth.informationmanager.common.transform.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.HashSet;
import java.util.Set;

@JsonPropertyOrder({"id","property","dataType","exactValue","oneOf","dataTypeRestriction"})
public class DataPropertyRange extends Axiom{
    private ConceptReference dataType;


    @JsonProperty("DataType")
    public ConceptReference getDataType() {
        return dataType;
    }

    public DataPropertyRange setDataType(ConceptReference dataType) {
        this.dataType = dataType;
        return this;
    }

    @JsonIgnore
    public DataPropertyRange setDataType(String dataType) {
        this.dataType = new ConceptReference(dataType);
        return this;
    }


}
