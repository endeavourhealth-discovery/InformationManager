package org.endeavourhealth.informationmanager.transforms.sources;

import java.util.ArrayList;
import java.util.List;

public class ComplexMapGroup {
   Integer groupNumber;
   List<ComplexMapTarget> targetMaps;

   public Integer getGroupNumber() {
      return groupNumber;
   }

   public ComplexMapGroup setGroupNumber(Integer groupNumber) {
      this.groupNumber = groupNumber;
      return this;
   }

   public List<ComplexMapTarget> getTargetMaps() {
      return targetMaps;
   }

   public ComplexMapGroup setTargetMaps(List<ComplexMapTarget> targetMaps) {
      this.targetMaps = targetMaps;
      return this;
   }
   public  ComplexMapGroup addTargetMap(ComplexMapTarget target){
      if (targetMaps==null)
         targetMaps = new ArrayList<>();
      targetMaps.add(target);
      return this;

   }
}
