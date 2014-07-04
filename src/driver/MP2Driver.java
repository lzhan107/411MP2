/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package driver;

import domain.PersistentObject;
import domain.PopulationBirthRecordComparator2010;
import domain.PopulationBirthRecordComparator2011;
import domain.PopulationDeathRecordComparator2010;
import domain.PopulationDeathRecordComparator2011;
import domain.PopulationRecord;
import domain.StatePopulationEstimateComparator2010;
import domain.StatePopulationEstimateComparator2011;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Lei
 */
public class MP2Driver {
    private static BufferedReader input = null;//Variable for reading the population data from data file
    private static PrintWriter output = null;//PrintStream variable for writing the population record into population-reocrd.txt file
    private static List<PopulationRecord> populationRecords = new ArrayList<>(); //ArrayList for storing each populationRecord array
    private static PopulationRecord[] populationRecord = new PopulationRecord[57]; //Array for storing each populationRecord
    private static PersistentObject persistentObject = null;//Variable for storing the timestamp and population record ArrayList
    private static ObjectOutputStream oos = null;//ObjectStream variable for writing object data into population-record.ser
    private static ObjectInputStream ois = null;//ObjectStream variable for reading object data into program
    private static long serializedTime = 0;//Time variable for storing the timestamp before serializing
    private static long deserializedTime = 0;//Time variable for storing the timestamp after serializing
    private static List<PopulationRecord> deserializedRecords = new ArrayList<>();//Variable for storing the deserialized objects
    private static PrintWriter statisticsOutput = null;//PrintStream variable for writing the statistical results in the mp2out.txt file
    /**
     * Below variables are for statistical analysis 1 - population %increase based on estimate per region per year
     * @param atgs 
     */
    private static int regionId;
    private static float NPopChng2010Region0 = 0;
    private static float NPopChng2011Region0 = 0;
    private static float NPopChng2010Region1 = 0;
    private static float NPopChng2011Region1 = 0;
    private static float NPopChng2010Region2 = 0;
    private static float NPopChng2011Region2 = 0;
    private static float NPopChng2010Region3 = 0;
    private static float NPopChng2011Region3 = 0;
    private static float NPopChng2010Region4 = 0;
    private static float NPopChng2011Region4 = 0;
    private static float estimateBase2010Region0 = 0;
    private static float popEstimate2011Region0 = 0;
    private static float estimateBase2010Region1 = 0;
    private static float popEstimate2011Region1 = 0;
    private static float estimateBase2010Region2 = 0;
    private static float popEstimate2011Region2 = 0;
    private static float estimateBase2010Region3 = 0;
    private static float popEstimate2011Region3 = 0;
    private static float estimateBase2010Region4 = 0;
    private static float popEstimate2011Region4 = 0;
    private static DecimalFormat df = new DecimalFormat("#0.##");
    /**
     * Below variables are for statistical analysis 4 & 5
     * - Number of states with estimated population increase
     * - Number of states with estimated population decrease
     * @param atgs 
     */
    private static int numOfStateWithEstimatePopInc2010 = 0;
    private static int numOfStateWithEstimatePopDes2010 = 0;
    private static int numOfStateWithEstimatePopInc2011 = 0;
    private static int numOfStateWithEstimatePopDes2011 = 0;
    
    public static void main(String[] atgs) {
        try {
            input = new BufferedReader(new FileReader("./data/NST_EST2011_ALLDATA.csv"));
            output = new PrintWriter(new FileWriter("./output/population-record.txt"));
            statisticsOutput = new PrintWriter("./data/mp2out.txt");
            String line;
            int prIndex = 0;
            boolean isHeadLine = true;
            while (null != (line = input.readLine())) {
                //Writing all data into population-record.txt
                output.write(line);
                output.write("\r\n");
                //Ignoring the headline
                if (isHeadLine) {
                    isHeadLine = false;
                    continue;
                }
                //Splitting the data, assign each line of data to a String array
                String[] details = line.split(",");
                for (int i = 0; i < 31; i++) {
                    if (details[i].equals("X")) {
                        details[i] = "-1";
                    }
                }
                int sumlev = Integer.parseInt(details[0]);
                int region = Integer.parseInt(details[1]);
                int division = Integer.parseInt(details[2]);
                int state = Integer.parseInt(details[3]);
                String name = details[4];
                long census2010pop = Long.parseLong(details[5]);
                long estimatesbase2010 = Long.parseLong(details[6]);
                long popestimate2010 = Long.parseLong(details[7]);
                long popestimate2011 = Long.parseLong(details[8]);
                long npopchg_2010 = Long.parseLong(details[9]);
                long npopchg_2011 = Long.parseLong(details[10]);
                long births2010 = Long.parseLong(details[11]);
                long births2011 = Long.parseLong(details[12]);
                long deaths2010 = Long.parseLong(details[13]);
                long deaths2011 = Long.parseLong(details[14]);
                long naturalinc2010 = Long.parseLong(details[15]);
                long naturalinc2011 = Long.parseLong(details[16]);
                long internationalmig2010 = Long.parseLong(details[17]);
                long internationalmig2011 = Long.parseLong(details[18]);
                long demosticmig2010 = Long.parseLong(details[19]);
                long demosticmig2011 = Long.parseLong(details[20]);
                long netmig2010 = Long.parseLong(details[21]);
                long netmig2011 = Long.parseLong(details[22]);
                long residual2010 = Long.parseLong(details[23]);
                long residual2011 = Long.parseLong(details[24]);
                double rbirth2011 = Double.parseDouble(details[25]);
                double rdeath2011 = Double.parseDouble(details[26]);
                double rnaturalinc2011 = Double.parseDouble(details[27]);
                double rinternationalimg2011 = Double.parseDouble(details[28]);
                double rdomesticimg2011 = Double.parseDouble(details[29]);
                double rnetimg2011 = Double.parseDouble(details[30]);
                //Initantiating the PopulationRecord intance
                populationRecord[prIndex] = new PopulationRecord(sumlev, region, division, state, name, census2010pop, 
                                                                 estimatesbase2010, popestimate2010, popestimate2011, 
                                                                 npopchg_2010, npopchg_2011,births2010, births2011, 
                                                                 deaths2010, deaths2011, naturalinc2010, naturalinc2011, 
                                                                 internationalmig2010, internationalmig2011, demosticmig2010,
                                                                 demosticmig2011, netmig2010, netmig2011, residual2010, 
                                                                 residual2011, rbirth2011, rdeath2011, rnaturalinc2011, 
                                                                 rinternationalimg2011, rdomesticimg2011, rnetimg2011);
                //Adding each PopultaionRecord object into the ArrayList
                populationRecords.add(populationRecord[prIndex]);
                prIndex++;
            }
            //Creating an instance of PersistentObject with the current timestamp and the ArrayList object.
            serializedTime = System.currentTimeMillis();
            persistentObject = new PersistentObject(serializedTime, populationRecords);
            //Serializing the persistent object to a file called population-record.ser
            oos = new ObjectOutputStream(new FileOutputStream("./data/population-record.ser"));
            oos.writeLong(persistentObject.getSerializedTime());
            oos.writeObject(persistentObject.getPopulationRecords());
            statisticsOutput.write("Application will now be sleeping for 5 seconds");
            statisticsOutput.write("\r\n");
            //Making the application sleep for 5 seconds
            Thread.sleep(5000);
            statisticsOutput.write("Applicaiton is waking up");
            statisticsOutput.write("\r\n");
            //Deserializing the persisted object into a date object and an ArrayList object called deserializedPopulationRecords
            ois = new ObjectInputStream(new FileInputStream("./data/population-record.ser"));
            deserializedTime = ois.readLong();
            deserializedRecords = (List<PopulationRecord>) ois.readObject();
            //Display the time difference between serialization and deserialization
            statisticsOutput.write("The time difference between serialization and deserialization is " + (System.currentTimeMillis() - deserializedTime) / 1000 + " seconds");
            statisticsOutput.write("\r\n");
            
            /**
             * Statistics 1
             * Population %increase based on estimate per region per year
             */
            for (int i = 0; i < deserializedRecords.size(); i++) {
                regionId = deserializedRecords.get(i).getRegion(); // Get ith element in the ArrayList
                if (regionId == 0) {
                    NPopChng2010Region0 += deserializedRecords.get(i).getNpopchg_2010();
                    NPopChng2011Region0 += deserializedRecords.get(i).getNpopchg_2011();
                    estimateBase2010Region0 += deserializedRecords.get(i).getEstimatesbase2010();
                    popEstimate2011Region0 += deserializedRecords.get(i).getPopestimate2011();
                } else if (regionId == 1) {
                    NPopChng2010Region1 += deserializedRecords.get(i).getNpopchg_2010();
                    NPopChng2011Region1 += deserializedRecords.get(i).getNpopchg_2011();
                    estimateBase2010Region1 += deserializedRecords.get(i).getEstimatesbase2010();
                    popEstimate2011Region1 += deserializedRecords.get(i).getPopestimate2011();
                } else if (regionId == 2) {
                    NPopChng2010Region2 += deserializedRecords.get(i).getNpopchg_2010();
                    NPopChng2011Region2 += deserializedRecords.get(i).getNpopchg_2011();
                    estimateBase2010Region2 += deserializedRecords.get(i).getEstimatesbase2010();
                    popEstimate2011Region2 += deserializedRecords.get(i).getPopestimate2011();
                } else if (regionId == 3) {
                    NPopChng2010Region3 += deserializedRecords.get(i).getNpopchg_2010();
                    NPopChng2011Region3 += deserializedRecords.get(i).getNpopchg_2011();
                    estimateBase2010Region3 += deserializedRecords.get(i).getEstimatesbase2010();
                    popEstimate2011Region3 += deserializedRecords.get(i).getPopestimate2011();
                } else if (regionId == 4) {
                    NPopChng2010Region4 += deserializedRecords.get(i).getNpopchg_2010();
                    NPopChng2011Region4 += deserializedRecords.get(i).getNpopchg_2011();
                    estimateBase2010Region4 += deserializedRecords.get(i).getEstimatesbase2010();
                    popEstimate2011Region4 += deserializedRecords.get(i).getPopestimate2011();
                }
            }
            statisticsOutput.write("1> Statistics for the population increase based on estimate");
            statisticsOutput.write("\r\n");
            statisticsOutput.write("   • For the year 2010, the population increases based on estimate are:");
            statisticsOutput.write("\r\n");
            for (int regionNum = 0; regionNum < 5; regionNum++) {
                if (regionNum == 0) {
                    statisticsOutput.write("   • For region " + regionNum + "," + "the increase rate is: " + df.format((NPopChng2010Region0 / estimateBase2010Region0) * 100) + "%");
                    statisticsOutput.write("\r\n");
                } else if (regionNum == 1) {
                    statisticsOutput.write("   • For region " + regionNum + "," + "the increase rate is: " + df.format((NPopChng2010Region1 / estimateBase2010Region1) * 100) + "%");
                    statisticsOutput.write("\r\n");
                } else if (regionNum == 2) {
                    statisticsOutput.write("   • For region " + regionNum + "," + "the increase rate is: " + df.format((NPopChng2010Region2 / estimateBase2010Region2) * 100) + "%");
                    statisticsOutput.write("\r\n");
                } else if (regionNum == 3) {
                    statisticsOutput.write("   • For region " + regionNum + "," + "the increase rate is: " + df.format((NPopChng2010Region3 / estimateBase2010Region3) * 100) + "%");
                    statisticsOutput.write("\r\n");
                } else {
                    statisticsOutput.write("   • For region " + regionNum + "," + "the increase rate is: " + df.format((NPopChng2010Region4 / estimateBase2010Region4) * 100) + "%");
                    statisticsOutput.write("\r\n");
                }
            }

            statisticsOutput.write("-----------------------------------------------------------------------------");
            statisticsOutput.write("\r\n");
            statisticsOutput.write("   • For the year 2011, the population increases based on estimate are:");
            statisticsOutput.write("\r\n");
            for (int regionNum = 0; regionNum < 5; regionNum++) {
                if (regionNum == 0) {
                    statisticsOutput.write("   • For region " + regionNum + "," + "the increase rate is: " + df.format((NPopChng2011Region0 / (popEstimate2011Region0 - NPopChng2011Region0)) * 100) + "%");
                    statisticsOutput.write("\r\n");
                } else if (regionNum == 1) {
                    statisticsOutput.write("   • For region " + regionNum + "," + "the increase rate is: " + df.format((NPopChng2011Region1 / (popEstimate2011Region1 - NPopChng2011Region1)) * 100) + "%");
                    statisticsOutput.write("\r\n");
                } else if (regionNum == 2) {
                    statisticsOutput.write("   • For region " + regionNum + "," + "the increase rate is: " + df.format((NPopChng2011Region2 / (popEstimate2011Region2 - NPopChng2011Region2)) * 100) + "%");
                    statisticsOutput.write("\r\n");
                } else if (regionNum == 3) {
                    statisticsOutput.write("   • For region " + regionNum + "," + "the increase rate is: " + df.format((NPopChng2011Region3 / (popEstimate2011Region3 - NPopChng2011Region3)) * 100) + "%");
                    statisticsOutput.write("\r\n");
                } else {
                    statisticsOutput.write("   • For region " + regionNum + "," + "the increase rate is: " + df.format((NPopChng2011Region4 / (popEstimate2011Region4 - NPopChng2011Region4)) * 100) + "%");
                }
            }
            statisticsOutput.write("\r\n");
            statisticsOutput.write("\r\n");
            /**
             * Statistics 2
             * Max and min births per state per year
             */
            //Get the sublist from the population record list, since we only need to consider the state
            statisticsOutput.write("2> Statistics for max & min births per state per year(Including Puerto Rico Commonwealth)");
            statisticsOutput.write("\r\n");
            Collections.sort(deserializedRecords.subList(5, 56), new PopulationBirthRecordComparator2010());
            statisticsOutput.write("   • For the year 2010, the state with minimum births is: " + deserializedRecords.subList(5, 56).get(0).getName() + " | " + "And the birth number is: " + deserializedRecords.subList(5, 56).get(0).getBirths2010());
            statisticsOutput.write("\r\n");
            statisticsOutput.write("   • For the year 2010, the state with maximum births is: " + deserializedRecords.subList(5, 56).get(50).getName() + " | " + "And the birth number is: " + deserializedRecords.subList(5, 56).get(50).getBirths2010());
            statisticsOutput.write("\r\n");

            Collections.sort(deserializedRecords.subList(5, 56), new PopulationBirthRecordComparator2011());
            statisticsOutput.write("   • For the year 2010, the state with minimum births is: " + deserializedRecords.subList(5, 56).get(0).getName() + " | " + "And the birth number is: " + deserializedRecords.subList(5, 56).get(0).getBirths2011());
            statisticsOutput.write("\r\n");
            statisticsOutput.write("\r\n");
            statisticsOutput.write("\r\n");
            /**
             * Statistics 3
             * Max and min deaths per state per year
             */
            statisticsOutput.write("3> Statistics for max & min deaths per state per year(Including Puerto Rico Commonwealth)");
            statisticsOutput.write("\r\n");
            Collections.sort(deserializedRecords.subList(5, 56), new PopulationDeathRecordComparator2010());
            statisticsOutput.write("   • For the year 2010, the state with minimum deaths is: " + deserializedRecords.subList(5, 56).get(0).getName() + " | " + "And the death number is: " + deserializedRecords.subList(5, 56).get(0).getDeaths2010());
            statisticsOutput.write("\r\n");
            statisticsOutput.write("   • For the year 2010, the state with minimum deaths is: " + deserializedRecords.subList(5, 56).get(50).getName() + " | " + "And the death number is: " + deserializedRecords.subList(5, 56).get(50).getDeaths2010());
            statisticsOutput.write("\r\n");
            
            Collections.sort(deserializedRecords.subList(5, 56), new PopulationDeathRecordComparator2011());
            statisticsOutput.write("   • For the year 2011, the state with minimum deaths is: " + deserializedRecords.subList(5, 56).get(0).getName() + " | " + "And the death number is: " + deserializedRecords.subList(5, 56).get(0).getDeaths2011());
            statisticsOutput.write("\r\n");
            statisticsOutput.write("   • For the year 2011, the state with minimum deaths is: " + deserializedRecords.subList(5, 56).get(50).getName() + " | " + "And the death number is: " + deserializedRecords.subList(5, 56).get(50).getDeaths2011());
            statisticsOutput.write("\r\n");
            statisticsOutput.write("\r\n");
            statisticsOutput.write("\r\n");

            /**
             * Statistics 4 & 5
             * Number of states with estimated population increase & Number of
             * states with estimated population decrease
             */
            for (int i = 0; i < deserializedRecords.subList(5, 56).size(); i++) {
                if (deserializedRecords.subList(5, 56).get(i).getNpopchg_2010() > 0) {
                    numOfStateWithEstimatePopInc2010++;
                } else if (deserializedRecords.subList(5, 56).get(i).getNpopchg_2010() < 0) {
                    numOfStateWithEstimatePopDes2010++;
                }
            }
            for (int i = 0; i < deserializedRecords.subList(5, 56).size(); i++) {
                if (deserializedRecords.subList(5, 56).get(i).getNpopchg_2011() > 0) {
                    numOfStateWithEstimatePopInc2011++;
                } else if (deserializedRecords.subList(5, 56).get(i).getNpopchg_2011() < 0) {
                    numOfStateWithEstimatePopDes2011++;
                }
            }
            statisticsOutput.write("4> Statistics for Number of states with estimated population increase(Including Puerto Rico Commonwealth)");
            statisticsOutput.write("\r\n");
            statisticsOutput.write("   • For the year 2010, the number of states with estimate population increase is: " + numOfStateWithEstimatePopInc2010);
            statisticsOutput.write("\r\n");
            statisticsOutput.write("   • For the year 2011, the number of states with estimate population increase is: " + numOfStateWithEstimatePopInc2011);
            statisticsOutput.write("\r\n");
            statisticsOutput.write("\r\n");
            statisticsOutput.write("5> Statistics for Number of states with estimated population decrease(Including Puerto Rico Commonwealth)");
            statisticsOutput.write("\r\n");
            statisticsOutput.write("   • For the year 2010, the number of states with estimate population decrease is: " + numOfStateWithEstimatePopDes2010);
            statisticsOutput.write("\r\n");
            statisticsOutput.write("   • For the year 2011, the number of states with estimate population decrease is: " + numOfStateWithEstimatePopDes2011);
            statisticsOutput.write("\r\n");
            statisticsOutput.write("\r\n");
            statisticsOutput.write("\r\n");
            /**
             * Statistics 6
             * State with most estimated population per year
             */
            Collections.sort(deserializedRecords.subList(5, 56), new StatePopulationEstimateComparator2010());
            statisticsOutput.write("6> Statistics for state with most estimated population per year(Including Puerto Rico Commonwealth)");
            statisticsOutput.write("\r\n");
            statisticsOutput.write("   • For the year 2010, the state with least estimated population is: " + deserializedRecords.subList(5, 56).get(0).getName() + " | " + "And the estimated population is: " + deserializedRecords.subList(5, 56).get(0).getPopestimate2010());
            statisticsOutput.write("\r\n");
            statisticsOutput.write("   • For the year 2010, the state with most estimated population is: " + deserializedRecords.subList(5, 56).get(50).getName() + " | " + "And the estimated population is: " + deserializedRecords.subList(5, 56).get(50).getPopestimate2010());
            statisticsOutput.write("\r\n");
            statisticsOutput.write("\r\n");
            statisticsOutput.write("\r\n");
            /**
             * Statistics 7
             * State with least estimated population per year
             */
            Collections.sort(deserializedRecords.subList(5, 56), new StatePopulationEstimateComparator2011());
            statisticsOutput.write("7> Statistics for state with least estimated population per year(Including Puerto Rico Commonwealth)");
            statisticsOutput.write("\r\n");
            statisticsOutput.write("   • For the year 2011, the state with least estimated population is: " + deserializedRecords.subList(5, 56).get(0).getName() + " | " + "And the estimated population is: " + deserializedRecords.subList(5, 56).get(0).getPopestimate2011());
            statisticsOutput.write("\r\n");
            statisticsOutput.write("   • For the year 2011, the state with most estimated population is: " + deserializedRecords.subList(5, 56).get(50).getName() + " | " + "And the estimated population is: " + deserializedRecords.subList(5, 56).get(50).getPopestimate2011());
            statisticsOutput.write("\r\n");
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (IOException | InterruptedException ioe) {
            System.out.println(ioe.getMessage());
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException ex) {
                    System.out.println(ex.getMessage());
                }
            }
            if (output != null) {
                output.close();
            }
            if (oos != null) {
                try {
                    oos.close();
                } catch (IOException ex) {
                    System.out.println(ex.getMessage());
                }
            }
            if (ois != null) {
                try {
                    ois.close();
                } catch (IOException ex) {
                    System.out.println(ex.getMessage());
                }
            }
            if (statisticsOutput != null){
                    statisticsOutput.close();
            }
        }
    }
}
