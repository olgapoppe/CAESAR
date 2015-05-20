package iogenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import run.Run;
import run.RunID;

/**
 * Output file generator generates files for validation of the output and performance charts.
 * @author Olga Poppe
 */
public class OutputFileGenerator {
	
	public static void write2File (HashMap<RunID,Run> runs1, HashMap<RunID,Run> runs2, int HP_frequency, int LP_frequency) { 
		
		try {
			/*
			long total_garbageCollectionTime = 0;
			long total_priorityMaintenanceTime = 0;*/
			
			/*** Output files for validation ***/
			File tollalerts_file = new File("../../tollalerts.dat");
			BufferedWriter tollalerts_output = new BufferedWriter(new FileWriter(tollalerts_file));		

			File accidentalerts_file = new File("../../accidentalerts.dat");
			BufferedWriter accidentalerts_output = new BufferedWriter(new FileWriter(accidentalerts_file)); 

			/*** Output files for experiments ***/
			/*File eventstorage_file = new File("../../eventstorage.dat");
			BufferedWriter eventstorage_output = new BufferedWriter(new FileWriter(eventstorage_file));

			File eventProcessingTimes_file = new File("../../eventprocessingtimes.dat");
			BufferedWriter eventProcessingTimes_output = new BufferedWriter(new FileWriter(eventProcessingTimes_file));

			File accidentProcessingTimes_file = new File("../../accidentprocessingtimes.dat");
			BufferedWriter accidentProcessingTimes_output = new BufferedWriter(new FileWriter(accidentProcessingTimes_file));  
			
			File times_file = new File("../../times.dat");
			BufferedWriter times_output = new BufferedWriter(new FileWriter(times_file));*/
			
			// Events processed and stored by runs
	     	Set<RunID> runids1 = runs1.keySet();
	     	for (RunID runid : runids1) {
	     		
	     		Run run = runs1.get(runid);	     		
	     		
	     		run.output.write2FileTollNotifications(tollalerts_output);
	     		run.output.write2FileAccidentWarnings(accidentalerts_output);
	     		
	     		/*run.write2FileEventStorage(eventstorage_output);
	     		run.output.write2FileEventProcessingTimes(eventProcessingTimes_output);
	     		run.write2FileAccidentProcessingTimes(accidentProcessingTimes_output);
	     		
	     		total_garbageCollectionTime += run.time.garbageCollectionTime;
	     		total_priorityMaintenanceTime += run.time.priorityMaintenanceTime;*/
	     	}	
	     	Set<RunID> runids2 = runs2.keySet();
	     	for (RunID runid : runids2) {
	     		
	     		Run run = runs2.get(runid);	     		
	     		
	     		run.output.write2FileTollNotifications(tollalerts_output);
	     		run.output.write2FileAccidentWarnings(accidentalerts_output);
	     		
	     		/*run.write2FileEventStorage(eventstorage_output);
	     		run.output.write2FileEventProcessingTimes(eventProcessingTimes_output);
	     		run.write2FileAccidentProcessingTimes(accidentProcessingTimes_output);
	     		
	     		total_garbageCollectionTime += run.time.garbageCollectionTime;
	     		total_priorityMaintenanceTime += run.time.priorityMaintenanceTime;*/
	     	}	
	        // Number of runs, total processing time, scheduling overhead, garbage collection overhead, priority maintenance overhead
	       /* String line = 	min_stream_rate + " " + max_stream_rate + " " + runs.size() + " " + 
	        				total_time + " " + total_garbageCollectionTime + " " + total_priorityMaintenanceTime + " " +
	        				HP_frequency + " " + LP_frequency + "\n";
	        times_output.write(line);*/
		
	        /*** Clean-up ***/
	       	tollalerts_output.close();
	       	accidentalerts_output.close();
	       	/*eventstorage_output.close();
	       	eventProcessingTimes_output.close();
	       	accidentProcessingTimes_output.close();
	       	times_output.close();*/
	       	
		} catch (IOException e) { e.printStackTrace(); }
	}
}
