package cs505finaltemplate.CEP;

import cs505finaltemplate.Launcher;
import cs505finaltemplate.httpcontrollers.API;
import io.siddhi.core.util.transport.InMemoryBroker;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private String topic;
    private Map<Integer, Integer> prevZipAlert;

    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
        this.prevZipAlert = new HashMap<>();
    }

    @Override
    public void onMessage(Object msg) {

        try {
            List<Integer> alertZipList = new ArrayList<>();

            System.out.println("OUTPUT CEP EVENT: " + msg);
            System.out.println("");

            //You will need to parse output and do other logic,
            //but this sticks the last output value in main
            Launcher.lastCEPOutput = String.valueOf(msg);

            //Parse Event Message
            String parsedMsg = String.valueOf(msg).replaceAll("\\[|\\{|}|]|\"event\":", "");
            parsedMsg = parsedMsg.replaceAll("\"zip_code\":|\"count\":|\"", "");
            String[] splitMsg = parsedMsg.split(",");

            //Build Alert String for each zip
            for (int i=0; i < splitMsg.length-1; i+=2){
                Integer zip = Integer.parseInt(splitMsg[i]);
                Integer newCount = Integer.parseInt(splitMsg[i+1]);

                //Check if zip was in previous event
                if (prevZipAlert.containsKey(zip)) {
                    //Check if zip's count has doubled in size
                    if (prevZipAlert.get(zip) * 2 <= newCount) {
                        //Add zip to alert list
                        alertZipList.add(zip);
                    }

                    //Set prevZipAlert to current event info
                    prevZipAlert.replace(zip, newCount);
                }
                else {
                    //Wasn't in the last event. Add it to the list.
                    prevZipAlert.put(zip, newCount);
                }
            }

            //Send alert list to API
            API.numAlertedZips = alertZipList.size();
            API.alertZipList = alertZipList.stream().mapToInt(Integer::intValue).toArray();


        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}
