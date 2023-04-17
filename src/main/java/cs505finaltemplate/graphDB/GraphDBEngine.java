package cs505finaltemplate.graphDB;

import java.util.Optional;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

public class GraphDBEngine {
    private static String dbName = "Basing the Data";
    private static String user = "root";
    private static String pass = "rootpwd";


    //!!! CODE HERE IS FOR EXAMPLE ONLY, YOU MUST CHECK AND MODIFY!!!
    public GraphDBEngine() {

        //launch a docker container for orientdb, don't expect your data to be saved unless you configure a volume
        //docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=rootpwd orientdb:3.0.0

        //use the orientdb dashboard to create a new database
        //see class notes for how to use the dashboard


        OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        ODatabaseSession db = orient.open("test", "root", "rootpwd");

        clearDB(db);

        //create classes
        OClass patient = db.getClass("patient");

        if (patient == null) {
            patient = db.createVertexClass("patient");
        }

        if (patient.getProperty("patient_mrn") == null) {
            patient.createProperty("patient_mrn", OType.STRING);
            patient.createIndex("patient_name_index", OClass.INDEX_TYPE.NOTUNIQUE, "patient_mrn");
        }

        if (db.getClass("contact_with") == null) {
            db.createEdgeClass("contact_with");
        }


        OVertex patient_0 = createPatient(db, "mrn_0");
        OVertex patient_1 = createPatient(db, "mrn_1");
        OVertex patient_2 = createPatient(db, "mrn_2");
        OVertex patient_3 = createPatient(db, "mrn_3");

        //patient 0 in contact with patient 1
        OEdge edge1 = patient_0.addEdge(patient_1, "contact_with");
        edge1.save();
        //patient 2 in contact with patient 0
        OEdge edge2 = patient_2.addEdge(patient_0, "contact_with");
        edge2.save();

        //you should not see patient_3 when trying to find contacts of patient 0
        OEdge edge3 = patient_3.addEdge(patient_2, "contact_with");
        edge3.save();

        getContacts(db, "mrn_0");

        db.close();
        orient.close();

    }

    //This creates a new patient vertex, it does not include the contact_list or event_list field because those will need to be calculated and updated later
    private OVertex createPatient(ODatabaseSession db, Integer testing_id, String patient_name, String patient_mrn, Boolean patient_status) {
        OVertex result = db.newVertex("patient");
        result.setProperty("testing_id", testing_id);
        result.setProperty("patient_name", patient_name);
        result.setProperty("patient_mrn", patient_mrn);
        result.setProperty("patient_status", patient_status); 
        result.save();
        return result;
    }

    //Creates a new hospital vertex, does not include patient information as that will be loaded into an edge
    private OVertex createHospital(ODatabaseSession db, Integer hospital_id) {
        OVertex result = db.newVertex("hospital");
        result.setProperty("hospital_id", hospital_id);
        result.save();
    }

    //Creates a new vaccination vertex, does not include patient information as it will be loaded into an edge
    private OVertex createVaccination(ODatabaseSession db, Integer vaccination_id) {
        OVertex result = db.newVertex("vaccination");
        result.setProperty("vaccination_id", vaccination_id);
        result.save();
    }

    //Creates a new event vertex, does not include patient information as it will be loaded into an edge
    private OVertex createEvent(ODatabaseSession db, String event_id) {
        OVertex result = db.newVertex("event");
        result.setProperty("event_id", event_id);
        result.save();
    }

    //Creates a contact edge between two patients
    private void createContactEdges(ODatabaseSession db, OVertex center_patient) {
        
    }

    //STILL NEEDS WORK
    //Searches the database for contacts between patients
    private void getContacts(ODatabaseSession db, String patient_mrn) {

        String query = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from patient where patient_mrn = ?) " +
                "WHILE $depth <= 2";
        OResultSet rs = db.query(query, patient_mrn);

        Optional<OVertex> first_patient = getPatientByMRN(db, patient_mrn); 

        while (rs.hasNext()) {
            OResult item = rs.next();
            if (item.hasProperty("patient_mrn")) {
                String temp = item.getProperty("patient_mrn");
                if (!patient_mrn.equals(temp)) {
                    resultList.add(temp);
                }
            }
            Optional<OVertex> contact = item.getVertex();
            System.out.println("contact: " + item.getProperty("patient_mrn"));

            first_patient.addEdge(contact, "contact_with");
        }

        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
    }

    //Creates a query to select a patient from the database given patient_mrn
    private Optional<OVertex> getPatientByMRN(ODatabaseSession db, String patient_mrn) {
        String query = "select * from patient where patient_mrn = ?"; //using * here is good in case there is duplicate data, might be useful for db cleansing later
        OResultSet rs = db.query(query, patient_mrn);
        
        Optional<OVertex> result = null;
        if (rs.hasNext()) {
            OResult patient = rs.next();
            result = patient.getVertex();
            System.out.println("contact: " + patient.getProperty("patient_mrn"));
        }
        rs.close();

        return result;
    }

    //Creates a query to select a patient from the database given the event_id
    private void getEventByID(ODatabaseSession db, String event_id) {
        String query = "select * from patient where event_id = ?"; //using * here is good in case there is duplicate data, might be useful for db cleansing later
        OResultSet rs = db.query(query, event_id);

        rs.close();
    }

    //Creates a query to select a patient from the database given the patient's mrn
    private void getHospitalByID(ODatabaseSession db, String hospital_id) {
        String query = "select * from patient where patient_mrn = ?"; //using * here is good in case there is duplicate data, might be useful for db cleansing later
        OResultSet rs = db.query(query, hospital_id);

        rs.close();
    }    

    //Removes values from database, but does not destroy the architecture
    private void clearDB(ODatabaseSession db) {

        String query = "DELETE VERTEX FROM patient";
        db.command(query);

    }

    private static int recreate(OrientDB client) {
        try {
            if (client.exists(dbName)) {
                client.drop(dbName); //if the database exists, we drop it
                System.out.println(dbName + " has been dropped. It will now be reset.");
            }
            client.create(dbName, ODatabaseType.PLOCAL, OrientDBConfig.defaultConfig()); //creates a new DB
            ODatabaseSession db = client.open(dbName, user, pass, OrientDBConfig.defaultConfig()); //open DB session
            build(db); //build DB
            db.close(); //close DB connection
            return 1;
        } catch (Exception ex) {
            System.out.println(ex);
            return 0;
        }
        
    }

    //Builds the database architecture needed to insert data using the createX functions which are written above.
    private static void build(ODatabaseSession db) {
        try {

            //Set up a vertex class for patient data JSON as specified in assignment
            OClass patient = db.getClass("patient"); //creates class based on specifications
            if (patient == null) {
                patient = db.createVertexClass("patient");
            }

            if (patient.getProperty("patient_mrn") == null) { //if mrn is null, we can populate
                patient.createProperty("testing_id", OType.INTEGER); //ID number of patient's testing facility
                patient.createProperty("patient_name", OType.STRING);
                patient.createProperty("patient_mrn", OType.STRING); //patient medical record number, should be unique                patient.createProperty("patient_zipcode", OType.INTEGER);
                patient.createProperty("patient_status", OType.BOOLEAN); //will either be 1 (positive) or 0 (negative)
                patient.createProperty("contact_list", OType.STRING); //contains MRNs of other patients that have been in known contact with this patient
                patient.createProperty("event_list", OType.STRING); //contains list of event_id that the person visited
            }

            //Set up a vertex class for hospital data JSON as specified in assignment
            OClass hospital = db.getClass("hospital");
            if (hospital == null) {
                hospital = db.createVertexClass("hospital");
            }

            if (hospital.getProperty("hospital_id") == null) { //if id is null, we can populate
                hospital.createProperty("hospital_id", OType.INTEGER); //ID number of hospital which is housing patient
                hospital.createProperty("patient_name", OType.STRING); //should correspond with a name in the patient class
                hospital.createProperty("patient_mrn", OType.STRING); //patient medical record number, should match one present in the patient class
                hospital.createProperty("patient_status", OType.INTEGER); //values can be 1 (in-patient), 2 (icu), or 3 (vent)
            }

            //Set up a vertex class for vaccination data JSON as specified in assignment
            OClass vaccination = db.getClass("hospital");
            if (vaccination == null) {
                vaccination = db.createVertexClass("vaccination");
            }

            if (vaccination.getProperty("vaccination_id") == null) { //if id is null, we can populate
                hospital.createProperty("vaccination_id", OType.INTEGER); //ID number of vaccine testing facility
                hospital.createProperty("patient_name", OType.STRING); //should correspond with a name in the patient class
                hospital.createProperty("patient_mrn", OType.STRING); //patient medical record number, should match one present in the patient class
            }

            //Create a vertex class for the tracked events
            OClass event = db.getClass("event");
            if (event == null) {
                event = db.createVertexClass("event");
            }

            if (event.getProperty("event_id") == null) {
                event.createProperty("event_id", OType.STRING);
            }

            //Create an edge class for patients that the patient was in contact with
            if (db.getClass("contact_with") == null) {
                db.createEdgeClass("contact_with");
            }

            //Create an edge class for events that the patient visited
            if  (db.getClass("event_visited") == null) {
                db.createEdgeClass("event_visited");
            }

            //Create an edge class for the hospital a patient is in
            if (db.getClass("hospital_at") == null) {
                db.createEdgeClass("hospital_at");
            }

            //Create an edge class for the vaccination station that a patient visited
            if (db.getClass("vaccination_at") == null) {
                db.createEdgeClass("vaccination_at");
            }
            
        } catch (Exception ex) { 
            System.out.println(ex);
        }
    }

    //Called in API to flush and rebuild the DB
    public static int reset() {
        int result = -1; //set to a value that can't be handled so we don't have an accidental reset

        try {
            OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
            result = recreate(orient);
            orient.close();
        } catch (Exception ex) {
            System.out.println(ex);
        }
        return result;
    }


}
