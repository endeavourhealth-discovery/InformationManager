package org.endeavourhealth.informationmanager.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import javafx.concurrent.Task;
import javafx.concurrent.WorkerStateEvent;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.stage.DirectoryChooser;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.TTDocumentFiler;
import org.endeavourhealth.informationmanager.TTDocumentFilerJDBC;
import org.endeavourhealth.informationmanager.common.transform.*;
import org.endeavourhealth.informationmanager.common.transform.exceptions.FileFormatException;
import org.endeavourhealth.informationmanager.transforms.SearchTermGenerator;
import org.semanticweb.owlapi.model.*;


import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.zip.DataFormatException;

public class MainController {


    public TextArea sqlEditor;
    public TextArea graphEditor;
    private Properties config;
    private Stage _stage;
    @FXML
    private TextArea logger;

    @FXML
    private TextField parentEntity;
    @FXML
    private TextField snomedNamespace;


    @FXML
    private ProgressBar progressBar;

    private Task conversionTask;
    private Thread conversionThread;
    private File inputFile;
    private File outputFile;
    private File inputFolder;
    private File outputFolder;

    public void setStage(Stage stage) {
        loadConfig();
        this._stage = stage;

    }

    private void loadConfig() {
        config = new Properties();
        try {
            FileInputStream cs = new FileInputStream("DiscoveryConverter.cfg");
            config.load(cs);
            String si = config.getProperty("inputFolderFolder");
            String so = config.getProperty("outputFolderFolder");
            String ns = config.getProperty("snomedNamespace");
            String pe = config.getProperty("parentEntity");
            String query= config.getProperty("graphEditorText");
            String sql= config.getProperty("sqlEditorText");

            snomedNamespace.setText((ns != null) ? ns : "1000252");
            parentEntity.setText((pe!=null) ? pe:"");
            graphEditor.setText((query!=null) ? query:getPrefixes());
            sqlEditor.setText((sql!=null) ? sql:null);




        } catch (FileNotFoundException nf) {

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void saveConfig(){

            config= new Properties();
            try {
                FileOutputStream cs = new FileOutputStream("DiscoveryConverter.cfg");
                config.setProperty("snomedNamespace",snomedNamespace.getText());
                config.setProperty("parentEntity",parentEntity.getText());
                config.setProperty("graphEditorText",graphEditor.getText());
                config.setProperty("sqlEditorText",sqlEditor.getText());
                config.store(cs,"saved configuration");


            } catch (FileNotFoundException nf) {

            } catch (IOException e) {
                e.printStackTrace();
            }


    }

    private void setIOFiles(String from,String to){
        FileChooser inFileChooser = new FileChooser();

        inFileChooser.setTitle("Select input ("+ from.toUpperCase()+") file");
        inFileChooser.getExtensionFilters()
                .add(
                        new FileChooser.ExtensionFilter(from.toUpperCase()+"Files","*."+from));

        inputFile = inFileChooser.showOpenDialog(_stage);
        if (inputFile == null)
            return;

        FileChooser outFileChooser = new FileChooser();
        outFileChooser.setTitle("Select output ("+ to.toUpperCase()+") file");
        outFileChooser.getExtensionFilters()
                .add(
                        new FileChooser.ExtensionFilter(to.toUpperCase()+ " Files", "*."+ to));
        outputFile = outFileChooser.showSaveDialog(_stage);
        if (outputFile == null)
            return;
    }

    @FXML
    protected void discoveryToOWL(ActionEvent event) {
        saveConfig();

        System.out.println("Discovery -> OWL");
        FileChooser inFileChooser = new FileChooser();

        inFileChooser.setTitle("Select input (JSON) file");
        inFileChooser.getExtensionFilters()
                .add(
                        new FileChooser.ExtensionFilter("JSON Files", "*.json")
                );
        File inputFile = inFileChooser.showOpenDialog(_stage);
        if (inputFile == null)
            return;

        FileChooser outFileChooser = new FileChooser();
        outFileChooser.setTitle("Select output (OWL) file");
        outFileChooser.getExtensionFilters()
                .add(
                        new FileChooser.ExtensionFilter("OWL Files", "*.owl")
                );
        File outputFile = outFileChooser.showSaveDialog(_stage);
        if (outputFile == null)
            return;

        try {
            clearlog();
            log("Initializing");
            TTManager dmanager= new TTManager();
            log("Loading JSON and transforming");
            TTDocument document= dmanager.loadDocument(inputFile);
            TTToOWLEL transformer= new TTToOWLEL();
            OWLOntologyManager owlManager= transformer.transform(document,dmanager);
            dmanager.saveOWLOntology(owlManager,outputFile);

            log("Done");
            alert("Transform complete", "Discovery -> OWL Transformer", "Transform finished");

        } catch (Exception e) {
            alert("Process complete", "Discovery-OWL Transformer", "No ontology created");
            ErrorController.ShowError(_stage, e);
        }

    }

    private void alert(String title, String header, String message) {
        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.setTitle(title);
        alert.setHeaderText(header);
        alert.setContentText(message);
        alert.showAndWait().ifPresent(rs -> {
            if (rs == ButtonType.OK) {
                System.out.println("Pressed OK.");
            }
        });
    }
    private boolean checkOK(String checkInfo) {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setHeaderText(checkInfo);
        Optional<ButtonType> option = alert.showAndWait();
        if (option.get() == ButtonType.OK)
            return true;
        return false;
    }


    private void clearlog() {
        logger.clear();
    }

    private void log(String message) {
        logger.textProperty().unbind();
        logger.appendText(message + "\n");
        logger.redo();
    }

    private File getOutputFile(String fileType) {
        FileChooser outFileChooser = new FileChooser();
        outFileChooser.setTitle("Select output (" + fileType + ") file");
        outFileChooser.getExtensionFilters()
                .add(
                        new FileChooser.ExtensionFilter(fileType + "Files"
                                , "*." + fileType.toLowerCase())
                );
        return outFileChooser.showSaveDialog(_stage);
    }

    private File getInputFile(String fileType) {
        FileChooser inFileChooser = new FileChooser();

        inFileChooser.setTitle("Select input (" + fileType + ") file");
        inFileChooser.getExtensionFilters()
                .add(
                        new FileChooser.ExtensionFilter(fileType + "Files"
                                , "*." + fileType.toLowerCase())
                );
        return inFileChooser.showOpenDialog(_stage);

    }


    public File getInputFolder() {

        DirectoryChooser chooser= new DirectoryChooser();
        chooser.setTitle("Select input folder");
        if (inputFolder!=null)
             chooser.setInitialDirectory(inputFolder);
        File of = chooser.showDialog(_stage);
        if (of!=null)
            inputFolder= of;
        return inputFolder;

    }





    private Task setTaskEvent(Task task , String taskDescription) {

        progressBar.setVisible(true);
        progressBar.setProgress(0);
        if (conversionThread!=null){
            conversionTask.cancel();
            conversionThread.interrupt();;
        }
        progressBar.progressProperty().unbind();
        progressBar.progressProperty().bind(task.progressProperty());
        logger.textProperty().bind(task.messageProperty());
        task.addEventHandler(WorkerStateEvent.WORKER_STATE_SUCCEEDED,
                (EventHandler<WorkerStateEvent>) t -> {
                    progressBar.setVisible(false);
                        log(taskDescription+ " completed ");
                        alert("Action complete", "", taskDescription);
                });
        task.addEventHandler(WorkerStateEvent.WORKER_STATE_FAILED,
                (EventHandler<WorkerStateEvent>) t -> {
                    progressBar.setVisible(false);
                    log("Problem with task see error");
                    alert("Action failed","Discovery converter ","unable to "+ taskDescription);
                    ErrorController.ShowError(_stage, (Exception) task.getException());                });
        return task;
    }





/*
    @FXML
   private void fileOntology(ActionEvent actionEvent) throws Exception {
        //Pair<String,String> user= loginAndFile();
        //if (user==null)
          //  return;
        fileDiscovery("","");
    }
*/

/*
    private void fileDiscovery(String id, String id2){
        File inputFile = getInputFile("json");
        if (inputFile!=null){

            try {

            OntologyImport.fileOntology(inputFile,false);
            log("Ontology filed and classification filed");
            alert("Ontology filer", "Discovery -> IMDB filer", "Filer finished");

            } catch (Exception e) {
                alert("Process complete", "Ontology filer ", "Not filed");
                ErrorController.ShowError(_stage, e);
            }

        }
   }
*/

/*    private Pair<String,String> loginAndFile() {
        // Create the custom dialog.
        Dialog<Pair<String, String>> dialog = new Dialog<>();
        dialog.setTitle("Data base credentials");
        dialog.setHeaderText("Enter user name and password");
        ButtonType loginButtonType = new ButtonType("Login", ButtonBar.ButtonData.OK_DONE);
        dialog.getDialogPane().getButtonTypes().addAll(loginButtonType, ButtonType.CANCEL);

        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(20, 150, 10, 10));

        TextField username = new TextField();
        username.setPromptText("Username");
        PasswordField password = new PasswordField();
        password.setPromptText("Password");

        grid.add(new Label("Username:"), 0, 0);
        grid.add(username, 1, 0);
        grid.add(new Label("Password:"), 0, 1);
        grid.add(password, 1, 1);
        Node loginButton = dialog.getDialogPane().lookupButton(loginButtonType);
        loginButton.setDisable(true);

        username.textProperty().addListener((observable, oldValue, newValue) -> {
            loginButton.setDisable(newValue.trim().isEmpty());
        });

        dialog.getDialogPane().setContent(grid);

        Platform.runLater(() -> username.requestFocus());

        dialog.setResultConverter(dialogButton -> {
            if (dialogButton == loginButtonType) {

                return new Pair<>(username.getText(), password.getText());
            }
            return null;
        });

        Optional<Pair<String, String>> result = dialog.showAndWait();
        if (result.isPresent())
            return result.get();
        else
            return null;

    }*/



    public void classifyDiscovery(ActionEvent actionEvent) throws Exception {
        File inputFile= getInputFile("json");
        if (inputFile!=null){
            File outputFile= getOutputFile("json");
            if (outputFile!=null){
                long start = System.currentTimeMillis();
                TTManager manager= new TTManager();
                TTDocument document= manager.loadDocument(inputFile);
                ReasonerPlus reasoner = new ReasonerPlus();
                reasoner.generateInferred(document);
                manager.saveDocument(outputFile);
                long end = System.currentTimeMillis();
                long duration= (end-start)/1000/60;
                log("Discovery file classified and saved in "+ String.valueOf(duration) + " minutes");
                alert("Classifier","Discovery ontology classify","completed");

            }
        }
    }



    public void runGraphQuery(ActionEvent actionEvent) {
        saveConfig();
        String queryText= graphEditor.getText();
       Repository db = new HTTPRepository("http://localhost:7200/", "InformationModel");
       RepositoryConnection conn = db.getConnection();

        if (queryText.contains("DELETE")) {
            Update deleteEntity = conn.prepareUpdate(queryText);
            ValueFactory vf = conn.getValueFactory();
            //deleteEntity.setBinding("entity", vf.createIRI("http://endhealth.info/im#903261000252100"));
            long start = System.currentTimeMillis();
            deleteEntity.execute();
            long end =System.currentTimeMillis();
            long duration = (end-start);
            logger.appendText("\n\n Duration = "+ duration+ " ms\n");

            logger.appendText("\n\nResult:\n");
            logger.appendText("Done");
        } else {
            TupleQuery query = conn.prepareTupleQuery(queryText);
            long start = System.currentTimeMillis();
            try (TupleQueryResult result = query.evaluate()) {
                int i = 0;
                while (result.hasNext()) {
                    BindingSet bindingSet = result.next();
                    i++;
                }
                result.close();
                conn.close();
                db.shutDown();
                long endResult= System.currentTimeMillis();
                logger.appendText("\n"+ i+" records found, duration with fetch= " + (endResult-start+" ms"));

            }
        }
    }
    private String getPrefixes(){
        String prefixes="PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
        +"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
        +"PREFIX : <http://endhealth.info/im#>\n"
        +"PREFIX dc: <http://purl.org/dc/elements/1.1/>\n"
        +"PREFIX owl: <http://www.w3.org/2002/07/owl#>\n"
        +"PREFIX sn: <http://snomed.info/sct#>\n"
        +"PREFIX sh: <http://www.w3.org/ns/shacl#>\n"
        +"PREFIX onto: <http://www.ontotext.com/>\n"
        +"PREFIX prov: <http://www.w3.org/ns/prov#>\n";
        return prefixes;

    }

    private List<org.eclipse.rdf4j.model.Namespace> getDefaultPrefixes(){
        List<org.eclipse.rdf4j.model.Namespace> prefixes= new ArrayList<>();
        prefixes.add(new SimpleNamespace("rdfs","http://www.w3.org/1999/02/22-rdf-syntax-ns#"));
        prefixes.add(new SimpleNamespace("rdf","http://www.w3.org/2000/01/rdf-schema#"));
        prefixes.add(new SimpleNamespace("im","http://endhealth.info/im#"));
        prefixes.add(new SimpleNamespace("dc","http://purl.org/dc/elements/1.1/"));
        prefixes.add(new SimpleNamespace("owl","http://www.w3.org/2002/07/owl#"));
        prefixes.add(new SimpleNamespace("sn","http://snomed.info/sct#"));
        prefixes.add(new SimpleNamespace("sh","http://www.w3.org/ns/shacl#"));
        prefixes.add(new SimpleNamespace("onto","http://www.ontotext.com/"));
        prefixes.add(new SimpleNamespace("prov","http://www.w3.org/ns/prov#"));
        return prefixes;
    }

   public void convertEcl(ActionEvent actionEvent) throws JsonProcessingException {
/*        String ecl= logger.getText();
        ECLToExpression eclConverter= new ECLToExpression();
        try {
            ClassExpression exp = eclConverter.getClassExpression(ecl);

       ObjectMapper objectMapper = new ObjectMapper();
       objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
       objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
       objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
       String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(exp);
        logger.appendText("\n\n"+ json);
        } catch (Exception e){
            logger.appendText("\n\n"+ e.toString());
        }*/
   }

    public void getEntityDefinition(ActionEvent actionEvent) throws JsonProcessingException {
        List<org.eclipse.rdf4j.model.Namespace> namespaces= getDefaultPrefixes();
        String siri= parentEntity.getText();
        Repository db = new HTTPRepository("http://localhost:7200/", "InformationModel");
        RepositoryConnection conn = db.getConnection();
        org.eclipse.rdf4j.model.IRI iri = IMAccess.getFullIri(siri,namespaces);
        List<String> display= IMAccess.getDisplayDefinition(conn,iri,namespaces);
        if (!display.isEmpty()){
            for (String item:display)
                logger.appendText("\n"+ item);
        }

    }


/*    public void getRDBEntity(ActionEvent actionEvent) throws JsonProcessingException {
        EntityServiceV3 entityService= new EntityServiceV3();
        Entity entity= entityService.getEntity(parentEntity.getText());

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(entity);
        logger.appendText("\n\n"+ json);
    }*/

    public void newSnomed(ActionEvent actionEvent) throws OWLOntologyCreationException, FileFormatException, IOException {
        File inputFile = getInputFile("json");
        if (inputFile != null) {
            try {
                TTManager manager= new TTManager();
                manager.loadDocument(inputFile);
                TTIriRef seedFrom= TTIriRef.iri(IM.NAMESPACE+"hasIncrementalFrom");
                TTEntity counter= manager.getEntity("im:890231000252108");
                if (counter.get(seedFrom)==null)
                    throw new FileFormatException("snomed counter not found");
                Integer seed= Integer.parseInt(counter.get(seedFrom).asLiteral().getValue());
                seed= seed+1;
                String snomed= SnomedConcept.createConcept(seed,false);
                counter.set(seedFrom, TTLiteral.literal(seed.toString()));
                manager.saveDocument(inputFile);
                logger.appendText("\n"+ snomed);

            } catch (Exception e) {
                logger.appendText("\n\n" + e.getStackTrace().toString());
            }
        }
    }


    public void getValueSetExpansion(ActionEvent actionEvent) throws DataFormatException {
       /* saveConfig();
        Repository db = new HTTPRepository("http://localhost:7200/", "InformationModel");
        RepositoryConnection con=db.getConnection();
        TupleQuery query = con.prepareTupleQuery("SELECT * WHERE {?S ?P ?O}");
        TupleQueryResult rs= query.evaluate();
        if (rs.hasNext())
            System.out.println("found");
        String valueSetIri= parentEntity.getText();
        Set<String> members = service.getValueSetExpansion(valueSetIri);
        logger.appendText("\n"+"Found "+ String.valueOf(members.size())+" entities. First 20 of which are:");
        for (int i=0; (i<members.size())&(i<10); i++)
            logger.appendText("\n"+ members.toArray()[i]);

        */

    }


    public void fileIM6(ActionEvent actionEvent) {
        File inputFile = getInputFile("json");
        if (inputFile!=null){

            try {

                System.out.println("Importing [" + inputFile + "]");
                ObjectMapper objectMapper = new ObjectMapper();

                TTDocument document = objectMapper.readValue(inputFile, TTDocument.class);
                System.out.println("Filing...");

                TTDocumentFiler filer = new TTDocumentFilerJDBC();

                filer.fileDocument(document,false,null);
                log("Ontology filed and classification filed");
                alert("Ontology filer", "Discovery -> IMDB filer", "Filer finished");

            } catch (Exception e) {
                alert("Process complete", "Ontology filer ", "Not filed");
                ErrorController.ShowError(_stage, e);
            }

        }
    }

    public void runSQLQuery(ActionEvent actionEvent) throws SQLException, ClassNotFoundException {
        Connection conn= IMAccess.getConnection();
        String queryText= sqlEditor.getText();
        PreparedStatement query = conn.prepareStatement(queryText);
        long start = System.currentTimeMillis();
        ResultSet rs= query.executeQuery();
        int i=0;
        while (rs.next()){
           i++;
        }
        rs.close();
        conn.close();
        long endResult= System.currentTimeMillis();
        logger .appendText("\n"+ i+ " records found, sql Duration with fetch= " + (endResult-start+" ms"));


    }

	public void termSearchGenerator(ActionEvent actionEvent) throws Exception {
      SearchTermGenerator.generateSearchTerms("c://CodeMaps");
	}
}
