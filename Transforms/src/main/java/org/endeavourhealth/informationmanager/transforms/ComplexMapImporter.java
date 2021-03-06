package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.ICD10;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.OPCS4;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.common.transform.TTManager;

import java.io.*;
import java.util.*;
import java.util.zip.DataFormatException;

/**
 * Imports the RF2 format extended complex backward mapping from snomed to ICD10 or OPCS 4
 *
 */
public class ComplexMapImporter {
    private static final String OPCS4_REFERENCE_SET = "1126441000000105";
    private static final String ICD10_REFERENCE_SET = "999002271000000101";

   private Map<String,List<ComplexMap>> snomedMap= new HashMap<>();
   private TTDocument document;
   private String refset;
   private Set<String> sourceCodes;
   private TTManager manager= new TTManager();

   /**
    * Imports a file containing the RF2 format extended complex backward maps between snomed and ICD10 or OPC4 4
    * @param file file including path
    * @param document the TTDocument already created with the graph name in place
    * @param refset the snomed reference set  id or the backward map set
    * @param sourceCodes a set of codes used to validate the map source entities.
    * A map will not be generated for any entity not in this set. Referential integrity for map source
    * @return the document populated with the complex maps
    * @throws IOException in the event of a file import problem
    * @throws  DataFormatException if the file content is invalid
    */
   public TTDocument importMap(File file, TTDocument document, String refset,Set<String> sourceCodes) throws IOException, DataFormatException {
      this.document= document;
      this.refset= refset;
      this.sourceCodes= sourceCodes;
      document.setCrud(IM.UPDATE);

      //imports file and creates snomed to target collection
      importFile(file);

      //takes the snomed maps creates reference entities and the 3 types of maps
      createEntityMaps();
      return document;
   }

   private void createEntityMaps() throws DataFormatException {
      Set<Map.Entry<String, List<ComplexMap>>> entries= snomedMap.entrySet();
      for (Map.Entry<String, List<ComplexMap>> entry:entries){
         String snomed= entry.getKey();
         if (sourceCodes.contains(snomed)) {
            List<ComplexMap> mapList = entry.getValue();
            setMapsForEntity(snomed, mapList);
         }

      }
   }

   private void setMapsForEntity(String snomed, List<ComplexMap> mapList) throws DataFormatException {
      TTEntity entity = new TTEntity().setIri((SNOMED.NAMESPACE + snomed));  // snomed entity reference
      document.addEntity(entity);
      entity.set(IM.HAS_MAP,new TTArray());
      if (mapList.size() == 1) {
         TTNode mapNode= new TTNode();
         entity.get(IM.HAS_MAP).asArray().add(mapNode);
         setMapNode(mapList.get(0),mapNode);
      } else {
         for (ComplexMap map : mapList) {
            TTNode mapNode = new TTNode();
            entity.get(IM.HAS_MAP).asArray().add(mapNode);
            setMapNode(map, mapNode);
         }
      }

   }

   private void setMapNode(ComplexMap map,TTNode oneMapNode) throws DataFormatException {
      if (map.getMapGroups().size() == 1) {
         setMapGroupNode(map.getMapGroups().get(0), oneMapNode,IM.SOME_OF);
      } else {
         oneMapNode.set(IM.COMBINATION_OF, new TTArray());
         for (ComplexMapGroup mapGroup : map.getMapGroups()) {
            TTNode groupNode = new TTNode();
            oneMapNode.get(IM.COMBINATION_OF).asArray().add(groupNode);
            setMapGroupNode(mapGroup, groupNode,IM.ONE_OF);
         }
      }
   }
   private void setMapGroupNode(ComplexMapGroup mapGroup, TTNode groupNode,TTIriRef oneOrSome) throws DataFormatException {
      if (mapGroup.getTargetMaps().size() == 1) {
         groupNode.set(oneOrSome,new TTArray());
          TTNode match= new TTNode();
         groupNode.get(oneOrSome).asArray().add(match);
         setTargetNode(mapGroup.getTargetMaps().get(0),match);
      } else {
         groupNode.set(oneOrSome,new TTArray());
         for (ComplexMapTarget mapTarget:mapGroup.getTargetMaps()) {
            TTNode match = new TTNode();
            groupNode.get(oneOrSome).asArray().add(match);
            setTargetNode(mapTarget,match);
         }

      }
   }


   private void setTargetNode(ComplexMapTarget mapTarget, TTNode match) throws DataFormatException {
      match.set(IM.MATCHED_TO, getTargetIri(mapTarget.target));
      if (mapTarget.getAdvice()!=null)
         match.set(IM.MAP_ADVICE,TTLiteral.literal(mapTarget.getAdvice()));
      if (mapTarget.getPriority()!=null)
         match.set(IM.MAP_PRIORITY,TTLiteral.literal((mapTarget.getPriority())));
      match.set(IM.ASSURANCE_LEVEL, IM.NATIONALLY_ASSURED);
   }



   private TTValue getTargetIri(String target) throws DataFormatException {
      if (refset.equals(OPCS4_REFERENCE_SET))
         return TTIriRef.iri(OPCS4.NAMESPACE + target);
      else if (refset.equals(ICD10_REFERENCE_SET))
         return TTIriRef.iri(ICD10.NAMESPACE + target);
      else
         throw new DataFormatException("unsupported map reference set");
   }

   private void importFile(File file) throws IOException {
      try(BufferedReader reader = new BufferedReader(new FileReader(file))){
         String line = reader.readLine();
         line = reader.readLine();
         int count=0;

         while (line!=null && !line.isEmpty()){

            String[] fields= line.split("\t");
            if (fields[4].equals(refset)&&"1".equals(fields[2])) {

               String snomed = fields[5];
               Integer group = Integer.parseInt(fields[6]);
               Integer priority = Integer.parseInt(fields[7]);
               String advice = fields[9];
               String target = fields[10];
               Integer block = Integer.parseInt(fields[12]);
               if (!target.contains("#")) {
                  addToMapSet(snomed,block,group,priority,advice,target);
                  count++;
                  if (count % 10000 == 0) {
                     System.out.println("Imported " + count + " complex maps");
                  }


               }
            }
            line = reader.readLine();
         }
         System.out.println(("imported "+count+" extended target maps resulting in " + snomedMap.size()+" snomed map entries"));
      }


   }

   private void addToMapSet(String snomed, Integer block, Integer group, Integer priority, String advice, String target) {
      ComplexMap map = getMap(snomed,block);
      ComplexMapGroup mapGroup = getMapGroup(map, group);
      ComplexMapTarget mapTarget = new ComplexMapTarget()
          .setPriority(priority)
          .setAdvice(advice)
          .setTarget(target);
      mapGroup.addTargetMap(mapTarget);
   }

   private ComplexMapGroup getMapGroup(ComplexMap map, Integer group){
      if (map.getMapGroups()==null){
         ComplexMapGroup mapGroup = new ComplexMapGroup();
         map.addMapGroup(mapGroup);
         mapGroup.setGroupNumber(group);
         return mapGroup;
      } else {
         for (ComplexMapGroup mapGroup:map.getMapGroups())
            if (mapGroup.getGroupNumber().equals(group))
               return mapGroup;
      }
      ComplexMapGroup mapGroup= new ComplexMapGroup();
      mapGroup.setGroupNumber(group);
      map.addMapGroup(mapGroup);
      return mapGroup;
   }

   private ComplexMap getMap(String snomed, Integer block) {
      if (snomedMap.get(snomed) == null) {
         List<ComplexMap> mapList = new ArrayList<>();
         snomedMap.put(snomed, mapList);
      }
      for (ComplexMap map:snomedMap.get(snomed)) {
         if (map.getMapNumber().equals(block))
               return map;
         }
      ComplexMap map = new ComplexMap();
      snomedMap.get(snomed).add(map);
      map.setMapNumber(block);
      return map;
   }

}
