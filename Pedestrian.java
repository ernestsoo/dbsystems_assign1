import java.util.Arrays;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.*;

public class Pedestrian{
     


    
    public Pedestrian(){

    }
    /**
     * This function will format the data for bulk import of Derby.
     */
    public void processDataDerby(){

        try {
           BufferedReader csvReader = new BufferedReader(new FileReader("data.del"));
           FileWriter fw = new FileWriter("data_derby.del");

           String row = csvReader.readLine();

           Integer counter = 0;

           while (row  != null ) {
               if (counter != 0){
                String[] data = row.split(",");
                
		String[] time = data[1].split(" ");
		String[] date = time[0].split("/");
		String hourStr = "";
		Integer hour = Integer.parseInt(data[6]);

		if (hour < 10){
		  hourStr = "0"+String.valueOf(hour);
		}else{
		  hourStr = String.valueOf(hour);
		}

                System.out.println(row);
		        String timestamp = date[2]+"-"+date[0]+"-"+date[1]+" "+hourStr+":00:01";
                fw.write(data[0]+","+timestamp+","+data[7]+","+data[9]+"\n");
                
               

                }
                row = csvReader.readLine();
                counter = counter + 1;
           }
           fw.close();
            
            
            
            csvReader.close();
       }
       catch (IOException ex) {
           ex.printStackTrace();
       }

     }

     /**
     * This function will format the data for bulk import of MongoDB.
     */
     public void processDataMongo(){

        try {
           BufferedReader csvReader = new BufferedReader(new FileReader("data.del"));
           FileWriter fw = new FileWriter("data_mongo.csv");
           String row = csvReader.readLine();
           Integer counter = 0;

           while (row  != null ) {
                String[] data = row.split(",");
               if (counter == 0){
                    fw.write(data[0]+","+data[2]+","+data[3]+","+data[4]+","+data[5]+","+data[6]+","+data[7]+","+data[8]+","+data[9]+"\n");
                }
                
               if (counter != 0){
                    String[] time = data[1].split(" ");
                    String[] date = time[0].split("/");
                    String hourStr = "";
                    Integer hour = Integer.parseInt(data[6]);

                    if (hour < 10){
                    hourStr = "0"+String.valueOf(hour);
                    }else{
                    hourStr = String.valueOf(hour);
                    }

                    System.out.println(row);
                    String timestamp = date[2]+"-"+date[0]+"-"+date[1]+"T"+hourStr+":00:01.000Z";
                    fw.write(data[0]+","+data[2]+","+date[1]+","+data[4]+","+data[5]+","+data[6]+","+data[7]+","+data[8]+","+data[9]+"\n");
                    }
                    row = csvReader.readLine();
                    counter = counter + 1;
                }
            fw.close();
            
            
            
            csvReader.close();
       }
       catch (IOException ex) {
           ex.printStackTrace();
       }

    }

    /**
    * This function will remove duplicate rows in the Sensors data.
    */
    public void makeSensorsUnique(){
        List<Integer> sensors = new ArrayList<Integer>();

        try {
           BufferedReader csvReader = new BufferedReader(new FileReader("sensor.del"));
           FileWriter fw = new FileWriter("sensor_unique.del");

           String row = csvReader.readLine();

           Integer counter = 0;

           while (row  != null ) {
               if (counter != 0){
                String[] data = row.split(",");
                
                if (!sensors.contains(Integer.parseInt(data[0]))){
                    System.out.println(row);
                    fw.write(data[0]+","+data[1]+"\n");
                    sensors.add(Integer.parseInt(data[0]));
                }
               

                }
                row = csvReader.readLine();
                counter = counter + 1;
           }
           fw.close();
            
            
            
            csvReader.close();
       }
       catch (IOException ex) {
           ex.printStackTrace();
       }

     }

    /**
    * This function will extract 2 columns into a new file (Sensor).
    */
     public void dataToSensors(){
        HashMap<String, String> sensors = new HashMap<String, String>();

        try{
            BufferedReader csvReader = new BufferedReader(new FileReader("data.del"));
            int counter = 0;
            String row = csvReader.readLine();
            FileWriter fw = new FileWriter("sensor.del");
            while (row  != null ) {
                String[] data = row.split(",");
                
                //counter++;
                System.out.println(row);
                
                //sensors.put(row[7],row[8]);
                
                fw.write(data[7] +',' + data[8]
                        +"\n");
                
                row = csvReader.readLine();

            }
                
        
 
            fw.close();
            
            
            
            csvReader.close();
        }
        catch (IOException ex) {
            ex.printStackTrace();
         }
     }
      
     public static void main(String[] args){
         Pedestrian pedestrian = new Pedestrian();

         //pedestrian.dataToSensors();
         //pedestrian.makeSensorsUnique();
         //pedestrian.processDataDerby();
         pedestrian.processDataMongo();
     }

     
}
