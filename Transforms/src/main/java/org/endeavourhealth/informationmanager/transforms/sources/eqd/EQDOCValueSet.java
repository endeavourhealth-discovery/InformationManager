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
import java.util.ArrayList;
import java.util.List;


/**
 * <p>Java class for EQDOC.ValueSet complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="EQDOC.ValueSet"&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;sequence&gt;
 *         &lt;element name="id" type="{http://www.e-mis.com/emisopen}dt.uid"/&gt;
 *         &lt;element name="codeSystem" type="{http://www.e-mis.com/emisopen}voc.CodeSystemEx"/&gt;
 *         &lt;element name="description" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/&gt;
 *         &lt;choice&gt;
 *           &lt;element name="allValues" type="{http://www.e-mis.com/emisopen}EQDOC.Exception"/&gt;
 *           &lt;element name="values" type="{http://www.e-mis.com/emisopen}EQDOC.ValueSetValue" maxOccurs="unbounded"/&gt;
 *         &lt;/choice&gt;
 *         &lt;element name="clusterCode" type="{http://www.w3.org/2001/XMLSchema}token" maxOccurs="unbounded" minOccurs="0"/&gt;
 *       &lt;/sequence&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "EQDOC.ValueSet", propOrder = {
    "id",
    "codeSystem",
    "description",
    "allValues",
    "values",
    "clusterCode"
})
public class EQDOCValueSet {

    @XmlElement(required = true)
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    @XmlSchemaType(name = "token")
    protected String id;
    @XmlElement(required = true)
    @XmlSchemaType(name = "token")
    protected VocCodeSystemEx codeSystem;
    protected String description;
    protected EQDOCException allValues;
    protected List<EQDOCValueSetValue> values;
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    @XmlSchemaType(name = "token")
    protected List<String> clusterCode;

    /**
     * Gets the value of the id property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the value of the id property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setId(String value) {
        this.id = value;
    }

    /**
     * Gets the value of the codeSystem property.
     * 
     * @return
     *     possible object is
     *     {@link VocCodeSystemEx }
     *     
     */
    public VocCodeSystemEx getCodeSystem() {
        return codeSystem;
    }

    /**
     * Sets the value of the codeSystem property.
     * 
     * @param value
     *     allowed object is
     *     {@link VocCodeSystemEx }
     *     
     */
    public void setCodeSystem(VocCodeSystemEx value) {
        this.codeSystem = value;
    }

    /**
     * Gets the value of the description property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getDescription() {
        return description;
    }

    /**
     * Sets the value of the description property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setDescription(String value) {
        this.description = value;
    }

    /**
     * Gets the value of the allValues property.
     * 
     * @return
     *     possible object is
     *     {@link EQDOCException }
     *     
     */
    public EQDOCException getAllValues() {
        return allValues;
    }

    /**
     * Sets the value of the allValues property.
     * 
     * @param value
     *     allowed object is
     *     {@link EQDOCException }
     *     
     */
    public void setAllValues(EQDOCException value) {
        this.allValues = value;
    }

    /**
     * Gets the value of the values property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the values property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getValues().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link EQDOCValueSetValue }
     * 
     * 
     */
    public List<EQDOCValueSetValue> getValues() {
        if (values == null) {
            values = new ArrayList<EQDOCValueSetValue>();
        }
        return this.values;
    }

    /**
     * Gets the value of the clusterCode property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the clusterCode property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getClusterCode().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link String }
     * 
     * 
     */
    public List<String> getClusterCode() {
        if (clusterCode == null) {
            clusterCode = new ArrayList<String>();
        }
        return this.clusterCode;
    }

}