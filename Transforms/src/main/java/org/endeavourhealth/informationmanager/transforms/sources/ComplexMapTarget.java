package org.endeavourhealth.informationmanager.transforms.sources;

public class ComplexMapTarget {
   Integer priority;
   String target;
   String advice;

   public Integer getPriority() {
      return priority;
   }

   public ComplexMapTarget setPriority(Integer priority) {
      this.priority = priority;
      return this;
   }

   public String getTarget() {
      return target;
   }

   public ComplexMapTarget setTarget(String target) {
      this.target = target;
      return this;
   }

   public String getAdvice() {
      return advice;
   }

   public ComplexMapTarget setAdvice(String advice) {
      this.advice = advice;
      return this;
   }
}
