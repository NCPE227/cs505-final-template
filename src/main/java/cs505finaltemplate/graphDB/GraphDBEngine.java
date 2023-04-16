package cs505finaltemplate.graphDB;

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

    private OVertex createPatient(ODatabaseSession db, String patient_mrn) {
        OVertex result = db.newVertex("patient");
        result.setProperty("patient_mrn", patient_mrn);
        result.save();
        return result;
    }

    private void getContacts(ODatabaseSession db, String patient_mrn) {

        String query = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from patient where patient_mrn = ?) " +
                "WHILE $depth <= 2";
        OResultSet rs = db.query(query, patient_mrn);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println("contact: " + item.getProperty("patient_mrn"));
        }

        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
    }

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
