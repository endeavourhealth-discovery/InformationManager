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
 * <p>Java class for EQDOC.CriteriaGroup complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="EQDOC.CriteriaGroup"&gt;
 *   &lt;complexContent&gt;
 *     &lt;extension base="{http://www.e-mis.com/emisopen}EQDOC.BaseCriteriaGroup"&gt;
 *       &lt;sequence&gt;
 *         &lt;element name="actionIfTrue" type="{http://www.e-mis.com/emisopen}voc.RuleAction"/&gt;
 *         &lt;element name="gotoIdIfTrue" type="{http://www.e-mis.com/emisopen}dt.uid" minOccurs="0"/&gt;
 *         &lt;element name="actionIfFalse" type="{http://www.e-mis.com/emisopen}voc.RuleAction"/&gt;
 *         &lt;element name="gotoIdIfFalse" type="{http://www.e-mis.com/emisopen}dt.uid" minOccurs="0"/&gt;
 *         &lt;element name="id" type="{http://www.e-mis.com/emisopen}dt.uid"/&gt;
 *       &lt;/sequence&gt;
 *     &lt;/extension&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "EQDOC.CriteriaGroup", propOrder = {
    "actionIfTrue",
    "gotoIdIfTrue",
    "actionIfFalse",
    "gotoIdIfFalse",
    "id"
})
public class EQDOCCriteriaGroup
    extends EQDOCBaseCriteriaGroup
{

    @XmlElement(required = true)
    @XmlSchemaType(name = "token")
    protected VocRuleAction actionIfTrue;
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    @XmlSchemaType(name = "token")
    protected String gotoIdIfTrue;
    @XmlElement(required = true)
    @XmlSchemaType(name = "token")
    protected VocRuleAction actionIfFalse;
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    @XmlSchemaType(name = "token")
    protected String gotoIdIfFalse;
    @XmlElement(required = true)
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    @XmlSchemaType(name = "token")
    protected String id;

    /**
     * Gets the value of the actionIfTrue property.
     * 
     * @return
     *     possible object is
     *     {@link VocRuleAction }
     *     
     */
    public VocRuleAction getActionIfTrue() {
        return actionIfTrue;
    }

    /**
     * Sets the value of the actionIfTrue property.
     * 
     * @param value
     *     allowed object is
     *     {@link VocRuleAction }
     *     
     */
    public void setActionIfTrue(VocRuleAction value) {
        this.actionIfTrue = value;
    }

    /**
     * Gets the value of the gotoIdIfTrue property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getGotoIdIfTrue() {
        return gotoIdIfTrue;
    }

    /**
     * Sets the value of the gotoIdIfTrue property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setGotoIdIfTrue(String value) {
        this.gotoIdIfTrue = value;
    }

    /**
     * Gets the value of the actionIfFalse property.
     * 
     * @return
     *     possible object is
     *     {@link VocRuleAction }
     *     
     */
    public VocRuleAction getActionIfFalse() {
        return actionIfFalse;
    }

    /**
     * Sets the value of the actionIfFalse property.
     * 
     * @param value
     *     allowed object is
     *     {@link VocRuleAction }
     *     
     */
    public void setActionIfFalse(VocRuleAction value) {
        this.actionIfFalse = value;
    }

    /**
     * Gets the value of the gotoIdIfFalse property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getGotoIdIfFalse() {
        return gotoIdIfFalse;
    }

    /**
     * Sets the value of the gotoIdIfFalse property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setGotoIdIfFalse(String value) {
        this.gotoIdIfFalse = value;
    }

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

}