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
 * <p>Java class for EQDOC.AggregateGroup complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="EQDOC.AggregateGroup"&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;sequence&gt;
 *         &lt;element name="id" type="{http://www.e-mis.com/emisopen}dt.uid"/&gt;
 *         &lt;element name="displayName" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/&gt;
 *         &lt;element name="groupingColumn" type="{http://www.w3.org/2001/XMLSchema}string" maxOccurs="unbounded"/&gt;
 *         &lt;element name="displayColumn" type="{http://www.e-mis.com/emisopen}EQDOC.AggregateDisplay" maxOccurs="unbounded" minOccurs="0"/&gt;
 *         &lt;element name="subTotals" type="{http://www.e-mis.com/emisopen}dt.bool"/&gt;
 *         &lt;element name="repeatHeader" type="{http://www.e-mis.com/emisopen}dt.bool" minOccurs="0"/&gt;
 *         &lt;choice minOccurs="0"&gt;
 *           &lt;element name="banding" type="{http://www.e-mis.com/emisopen}EQDOC.AggregateBanding"/&gt;
 *           &lt;element name="libraryItem" type="{http://www.e-mis.com/emisopen}dt.uid"/&gt;
 *         &lt;/choice&gt;
 *       &lt;/sequence&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "EQDOC.AggregateGroup", propOrder = {
    "id",
    "displayName",
    "groupingColumn",
    "displayColumn",
    "subTotals",
    "repeatHeader",
    "banding",
    "libraryItem"
})
public class EQDOCAggregateGroup {

    @XmlElement(required = true)
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    @XmlSchemaType(name = "token")
    protected String id;
    protected String displayName;
    @XmlElement(required = true)
    protected List<String> groupingColumn;
    protected List<EQDOCAggregateDisplay> displayColumn;
    protected boolean subTotals;
    @XmlElement(defaultValue = "false")
    protected Boolean repeatHeader;
    protected EQDOCAggregateBanding banding;
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    @XmlSchemaType(name = "token")
    protected String libraryItem;

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
     * Gets the value of the displayName property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getDisplayName() {
        return displayName;
    }

    /**
     * Sets the value of the displayName property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setDisplayName(String value) {
        this.displayName = value;
    }

    /**
     * Gets the value of the groupingColumn property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the groupingColumn property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getGroupingColumn().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link String }
     * 
     * 
     */
    public List<String> getGroupingColumn() {
        if (groupingColumn == null) {
            groupingColumn = new ArrayList<String>();
        }
        return this.groupingColumn;
    }

    /**
     * Gets the value of the displayColumn property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the displayColumn property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getDisplayColumn().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link EQDOCAggregateDisplay }
     * 
     * 
     */
    public List<EQDOCAggregateDisplay> getDisplayColumn() {
        if (displayColumn == null) {
            displayColumn = new ArrayList<EQDOCAggregateDisplay>();
        }
        return this.displayColumn;
    }

    /**
     * Gets the value of the subTotals property.
     * 
     */
    public boolean isSubTotals() {
        return subTotals;
    }

    /**
     * Sets the value of the subTotals property.
     * 
     */
    public void setSubTotals(boolean value) {
        this.subTotals = value;
    }

    /**
     * Gets the value of the repeatHeader property.
     * 
     * @return
     *     possible object is
     *     {@link Boolean }
     *     
     */
    public Boolean isRepeatHeader() {
        return repeatHeader;
    }

    /**
     * Sets the value of the repeatHeader property.
     * 
     * @param value
     *     allowed object is
     *     {@link Boolean }
     *     
     */
    public void setRepeatHeader(Boolean value) {
        this.repeatHeader = value;
    }

    /**
     * Gets the value of the banding property.
     * 
     * @return
     *     possible object is
     *     {@link EQDOCAggregateBanding }
     *     
     */
    public EQDOCAggregateBanding getBanding() {
        return banding;
    }

    /**
     * Sets the value of the banding property.
     * 
     * @param value
     *     allowed object is
     *     {@link EQDOCAggregateBanding }
     *     
     */
    public void setBanding(EQDOCAggregateBanding value) {
        this.banding = value;
    }

    /**
     * Gets the value of the libraryItem property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getLibraryItem() {
        return libraryItem;
    }

    /**
     * Sets the value of the libraryItem property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setLibraryItem(String value) {
        this.libraryItem = value;
    }

}