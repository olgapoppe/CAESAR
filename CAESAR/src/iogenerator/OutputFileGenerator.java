package iogenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import run.*;

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
	public static void write2File (ArrayList<HashMap<RunID,Run>> runtables, int HP_frequency, int LP_frequency, int lastSec, boolean count_and_rate) { 
		
		try {
			/*
			long total_garbageCollectionTime = 0;
			long total_priorityMaintenanceTime = 0;*/
			
			/*** Output files for validation ***/
			String output = "../../output/";
			String counts = "../../output/counts/";
			String rates = "../../output/rates/";
			
			// Total event counts
			File pr_counts_file = new File(counts + "pr_counts.dat");
			BufferedWriter pr_counts_output = new BufferedWriter(new FileWriter(pr_counts_file));
			
			File max_num_stored_events_file = new File(counts + "max_num_stored_events.dat");
			BufferedWriter max_num_stored_events_output = new BufferedWriter(new FileWriter(max_num_stored_events_file));		
			
			File rtn_counts_file = new File(counts + "rtn_counts.dat");
			BufferedWriter rtn_counts_output = new BufferedWriter(new FileWriter(rtn_counts_file));
			
			File ztn_counts_file = new File(counts + "ztn_counts.dat");
			BufferedWriter ztn_counts_output = new BufferedWriter(new FileWriter(ztn_counts_file));
			
			File aw_counts_file = new File(counts + "aw_counts.dat");
			BufferedWriter aw_counts_output = new BufferedWriter(new FileWriter(aw_counts_file));
			
			// Event rates
			File pr_rates_file = new File(rates + "pr_rates.dat");
			BufferedWriter pr_rates_output = new BufferedWriter(new FileWriter(pr_rates_file));
			
			File rtn_rates_file = new File(rates + "rtn_rates.dat");
			BufferedWriter rtn_rates_output = new BufferedWriter(new FileWriter(rtn_rates_file));
			
			File ztn_rates_file = new File(rates + "ztn_rates.dat");
			BufferedWriter ztn_rates_output = new BufferedWriter(new FileWriter(ztn_rates_file));
			
			File aw_rates_file = new File(rates + "aw_rates.dat");
			BufferedWriter aw_rates_output = new BufferedWriter(new FileWriter(aw_rates_file));
			
			// Complex events
			File tollalerts_file = new File(output + "tollalerts.dat");
			BufferedWriter tollalerts_output = new BufferedWriter(new FileWriter(tollalerts_file));		

			File accidentalerts_file = new File(output + "accidentalerts.dat");
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
					int lastMin =  new Double(Math.floor(lastSec/60) + 1).intValue();
					
					if (count_and_rate) {
						if (runid.xway == 0 && runid.dir == 0) run.output.writeEventCounts2File(seg, pr_counts_output, max_num_stored_events_output, rtn_counts_output, ztn_counts_output, aw_counts_output);	
						if (runid.xway == 0 && runid.dir == 1 && runid.seg == 85) run.output.writeStreamRates2File(pr_rates_output, rtn_rates_output, ztn_rates_output, aw_rates_output, lastMin);
					}
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
			rtn_counts_output.close();
			ztn_counts_output.close();
			aw_counts_output.close();
			
			pr_rates_output.close();
			rtn_rates_output.close();
			ztn_rates_output.close();
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
