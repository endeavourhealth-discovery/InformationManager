package org.endeavourhealth.informationmanager.common.transform.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class Clazz extends Concept {
    private List<ClassExpression> subClassOf;
    private List<ClassExpression> equivalentTo;
    private List<ClassExpression> disjointWithClass;
    private List<String> isA;

    @JsonProperty("SubClassOf")
    public List<ClassExpression> getSubClassOf() {
        return subClassOf;
    }

    public Clazz setSubClassOf(List<ClassExpression> subClassOf) {
        this.subClassOf = subClassOf;
        return this;
    }

    public Clazz addSubClassOf(ClassExpression subClassOf) {
        if (this.subClassOf == null)
            this.subClassOf = new ArrayList<>();

        this.subClassOf.add(subClassOf);
        return this;
    }

    @JsonProperty("EquivalentTo")
    public List<ClassExpression> getEquivalentTo() {
        return equivalentTo;
    }

    public Clazz setEquivalentTo(List<ClassExpression> equivalentTo) {
        this.equivalentTo = equivalentTo;
        return this;
    }

    public Clazz addEquivalentTo(ClassExpression equivalentTo) {
        if (this.equivalentTo == null)
            this.equivalentTo = new ArrayList<>();
        this.equivalentTo.add(equivalentTo);
        return this;
    }

    @JsonProperty("DisjointWithClass")
    public List<ClassExpression> getDisjointWithClass() {
        return disjointWithClass;
    }

    public Clazz setDisjointWithClass(List<ClassExpression> disjointWithClass) {
        this.disjointWithClass = disjointWithClass;
        return this;
    }

    public Clazz addDisjointClass(ClassExpression disjointWith) {
        if (this.disjointWithClass == null)
            this.disjointWithClass = new ArrayList<>();
        this.disjointWithClass.add(disjointWith);
        return this;
    }

    public Clazz addAllDisjointClasses(List<ClassExpression> disjointWith) {
        if (this.disjointWithClass == null)
            this.disjointWithClass = new ArrayList<>();
        this.disjointWithClass.addAll(disjointWith);
        return this;
    }

    @JsonProperty("isA")
    public List<String> getIsA() {
        return isA;
    }

    public Clazz setIsA(List<String> isA) {
        this.isA = isA;
        return this;
    }
}
