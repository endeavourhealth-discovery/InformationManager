//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, v2.3.2 
// See <a href="https://javaee.github.io/jaxb-v2/">https://javaee.github.io/jaxb-v2/</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2021.12.23 at 09:36:56 AM GMT 
//


package org.endeavourhealth.informationmanager.transforms.sources.eqd;

import javax.xml.bind.annotation.*;
import javax.xml.bind.annotation.adapters.CollapsedStringAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;


/**
 * <p>Java class for EQDOC.BandSingleValue complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="EQDOC.BandSingleValue"&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;sequence&gt;
 *         &lt;element name="value" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
 *         &lt;choice minOccurs="0"&gt;
 *           &lt;element name="datetimeUnit" type="{http://www.e-mis.com/emisopen}voc.BandDateTimeUnit"/&gt;
 *           &lt;element name="timespanUnit" type="{http://www.e-mis.com/emisopen}voc.BandTimespanUnit"/&gt;
 *         &lt;/choice&gt;
 *         &lt;element name="relation" type="{http://www.e-mis.com/emisopen}voc.Relation"/&gt;
 *         &lt;element name="displayText" type="{http://www.w3.org/2001/XMLSchema}token" minOccurs="0"/&gt;
 *       &lt;/sequence&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "EQDOC.BandSingleValue", propOrder = {
    "value",
    "datetimeUnit",
    "timespanUnit",
    "relation",
    "displayText"
})
public class EQDOCBandSingleValue {

    @XmlElement(required = true)
    protected String value;
    @XmlSchemaType(name = "token")
    protected VocBandDateTimeUnit datetimeUnit;
    @XmlSchemaType(name = "token")
    protected VocBandTimespanUnit timespanUnit;
    @XmlElement(required = true)
    @XmlSchemaType(name = "string")
    protected VocRelation relation;
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    @XmlSchemaType(name = "token")
    protected String displayText;

    /**
     * Gets the value of the value property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getValue() {
        return value;
    }

    /**
     * Sets the value of the value property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setValue(String value) {
        this.value = value;
    }

    /**
     * Gets the value of the datetimeUnit property.
     * 
     * @return
     *     possible object is
     *     {@link VocBandDateTimeUnit }
     *     
     */
    public VocBandDateTimeUnit getDatetimeUnit() {
        return datetimeUnit;
    }

    /**
     * Sets the value of the datetimeUnit property.
     * 
     * @param value
     *     allowed object is
     *     {@link VocBandDateTimeUnit }
     *     
     */
    public void setDatetimeUnit(VocBandDateTimeUnit value) {
        this.datetimeUnit = value;
    }

    /**
     * Gets the value of the timespanUnit property.
     * 
     * @return
     *     possible object is
     *     {@link VocBandTimespanUnit }
     *     
     */
    public VocBandTimespanUnit getTimespanUnit() {
        return timespanUnit;
    }

    /**
     * Sets the value of the timespanUnit property.
     * 
     * @param value
     *     allowed object is
     *     {@link VocBandTimespanUnit }
     *     
     */
    public void setTimespanUnit(VocBandTimespanUnit value) {
        this.timespanUnit = value;
    }

    /**
     * Gets the value of the relation property.
     * 
     * @return
     *     possible object is
     *     {@link VocRelation }
     *     
     */
    public VocRelation getRelation() {
        return relation;
    }

    /**
     * Sets the value of the relation property.
     * 
     * @param value
     *     allowed object is
     *     {@link VocRelation }
     *     
     */
    public void setRelation(VocRelation value) {
        this.relation = value;
    }

    /**
     * Gets the value of the displayText property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getDisplayText() {
        return displayText;
    }

    /**
     * Sets the value of the displayText property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setDisplayText(String value) {
        this.displayText = value;
    }

}