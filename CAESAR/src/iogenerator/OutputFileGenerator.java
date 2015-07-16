package iogenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import run.Run;
import run.RunID;

/**
 * Output file generator generates files for validation of the output and performance charts.
 * @author Olga Poppe
 */
public class OutputFileGenerator {
	
	/**
	 * Generates output files 
	 * @param runtables
	 * @param HP_frequency
	 * @param LP_frequency
	 */
	public static void write2File (ArrayList<HashMap<RunID,Run>> runtables, int HP_frequency, int LP_frequency, int lastSec) { 
		
		try {
			/*
			long total_garbageCollectionTime = 0;
			long total_priorityMaintenanceTime = 0;*/
			
			/*** Output files for validation ***/
			String path = "../../output/";
			
			// Total event counts
			File pr_counts_file = new File(path + "pr_counts.dat");
			BufferedWriter pr_counts_output = new BufferedWriter(new FileWriter(pr_counts_file));
			
			File max_num_stored_events_file = new File(path + "max_num_stored_events.dat");
			BufferedWriter max_num_stored_events_output = new BufferedWriter(new FileWriter(max_num_stored_events_file));		
			
			File tn_counts_file = new File(path + "tn_counts.dat");
			BufferedWriter tn_counts_output = new BufferedWriter(new FileWriter(tn_counts_file));
			
			File aw_counts_file = new File(path + "aw_counts.dat");
			BufferedWriter aw_counts_output = new BufferedWriter(new FileWriter(aw_counts_file));
			
			// Event rates
			File pr_rates_file = new File(path + "pr_rates.dat");
			BufferedWriter pr_rates_output = new BufferedWriter(new FileWriter(pr_rates_file));
			
			File tn_rates_file = new File(path + "tn_rates.dat");
			BufferedWriter tn_rates_output = new BufferedWriter(new FileWriter(tn_rates_file));
			
			File aw_rates_file = new File(path + "aw_rates.dat");
			BufferedWriter aw_rates_output = new BufferedWriter(new FileWriter(aw_rates_file));
			
			// Complex events
			File tollalerts_file = new File(path + "tollalerts.dat");
			BufferedWriter tollalerts_output = new BufferedWriter(new FileWriter(tollalerts_file));		

			File accidentalerts_file = new File(path + "accidentalerts.dat");
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
			for (HashMap<RunID,Run> runs : runtables) {
				
				Set<RunID> runids = runs.keySet();
				
				for (RunID runid : runids) {
	     		
					Run run = runs.get(runid);						
					int seg = new Double(runid.seg).intValue();
					
					if (runid.xway == 0 && runid.dir == 1) run.output.writeEventCounts2File(seg, pr_counts_output, max_num_stored_events_output, tn_counts_output, aw_counts_output);	
					if (runid.xway == 0 && runid.dir == 1 && runid.seg == 85) run.output.writeStreamRates2File(pr_rates_output, tn_rates_output, aw_rates_output, lastSec);
					run.output.writeTollNotifications2File(tollalerts_output);
					run.output.writeAccidentWarnings2File(accidentalerts_output);
	     		
					/*run.write2FileEventStorage(eventstorage_output);
	     			run.output.write2FileEventProcessingTimes(eventProcessingTimes_output);
	     			run.write2FileAccidentProcessingTimes(accidentProcessingTimes_output);
	     		
	     			total_garbageCollectionTime += run.time.garbageCollectionTime;
	     			total_priorityMaintenanceTime += run.time.priorityMaintenanceTime;*/
	     	}}		     	
	        // Number of runs, total processing time, scheduling overhead, garbage collection overhead, priority maintenance overhead
	       /* String line = 	min_stream_rate + " " + max_stream_rate + " " + runs.size() + " " + 
	        				total_time + " " + total_garbageCollectionTime + " " + total_priorityMaintenanceTime + " " +
	        				HP_frequency + " " + LP_frequency + "\n";
	        times_output.write(line);*/
		
	        /*** Clean-up ***/
			pr_counts_output.close();
			max_num_stored_events_output.close();
			tn_counts_output.close();
			aw_counts_output.close();
			
			pr_rates_output.close();
			tn_rates_output.close();
			aw_rates_output.close();
			
	       	tollalerts_output.close();
	       	accidentalerts_output.close();
	       	/*eventstorage_output.close();
	       	eventProcessingTimes_output.close();
	       	accidentProcessingTimes_output.close();
	       	times_output.close();*/
	       	
		} catch (IOException e) { e.printStackTrace(); }
	}
}
