package    org.endeavourhealth.imports.sources;

import org.endeavourhealth.imapi.model.tripletree.TTIriRef;

import java.util.ArrayList;
import java.util.List;

public class ComplexMap {
   private String source;
   private TTIriRef targetScheme;
   private Integer mapNumber;
   private List<ComplexMapGroup> mapGroups;

   public String getSource() {
      return source;
   }

   public ComplexMap setSource(String source) {
      this.source = source;
      return this;
   }

   public TTIriRef getTargetScheme() {
      return targetScheme;
   }

   public ComplexMap setTargetScheme(TTIriRef targetScheme) {
      this.targetScheme = targetScheme;
      return this;
   }



   public List<ComplexMapGroup> getMapGroups() {
      return mapGroups;
   }

   public ComplexMap setMapGroups(List<ComplexMapGroup> mapGroups) {
      this.mapGroups = mapGroups;
      return this;
   }

   public ComplexMap addMapGroup(ComplexMapGroup mapGroup){
      if (this.mapGroups==null)
         this.mapGroups= new ArrayList<>();
      mapGroups.add(mapGroup);
      return this;
   }

   public Integer getMapNumber() {
      return mapNumber;
   }

   public ComplexMap setMapNumber(Integer mapNumber) {
      this.mapNumber = mapNumber;
      return this;
   }
}
