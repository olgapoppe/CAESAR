package run;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import event.*;

/** 
 * Every run maintains its output consisting of processed position reports, generated toll notifications and accident warnings,
 * number of processed events and maximal number of stored events. 
 * @author Olga Poppe
 */
public class Output {
	
	// Intermediate storage of complex events
	public ArrayList<TollNotification> tollNotifications;
	public ArrayList<AccidentWarning> accidentWarnings;
	
	// Stream statistics
	public int position_reports_count;
	public int maxLengthOfEventQueue;
	public int real_toll_count;
	public int zero_toll_count;
	
	HashMap<Double,Integer> position_report_rates;
	HashMap<Double,Integer> real_toll_notification_rates;
	HashMap<Double,Integer> zero_toll_notification_rates;
	HashMap<Double,Integer> accident_warning_rates;
	
	public double count;
	public double sum;
	
	public Output () {
		
		//positionReports  = new ArrayList<PositionReport>();
		tollNotifications  = new ArrayList<TollNotification>();
		accidentWarnings = new ArrayList<AccidentWarning>();
		
		position_reports_count = 0;
		maxLengthOfEventQueue = 0;
		real_toll_count = 0;
		zero_toll_count = 0;
		
		position_report_rates = new HashMap<Double,Integer>();
		real_toll_notification_rates = new HashMap<Double,Integer>();
		zero_toll_notification_rates = new HashMap<Double,Integer>();
		accident_warning_rates = new HashMap<Double,Integer>();
		
		count = 0;
		sum = 0;
	}
	
	/**
	 * Write the processing times of position reports to the given file.  
	 * @param file
	 */	
	/*public void write2FileEventProcessingTimes (BufferedWriter file) {
		for (PositionReport p : positionReports) {
			p.write2FileEventProcessingTime(file);
		}  
	}*/
	
	/**
	 * Position report rate is maintained only for one run with identifier 0,1,85.  
	 * @param runid
	 * @param minute
	 */	
	public void update_positionreport_rates (RunID runid, double min) {
		if (runid.xway == 0 && runid.dir == 1 && runid.seg == 85) {
			if (position_report_rates.containsKey(min)) {
				int count = position_report_rates.get(min);
				position_report_rates.put(min, count+1);
			} else {
				position_report_rates.put(min,1);
		}}
	}
	
	/**
	 * Real toll notification rate is maintained for only one run with identifier 0,1,85.  
	 * @param runid
	 * @param minute
	 */
	public void update_real_tollnotification_rates (RunID runid, double min) {
		if (runid.xway == 0 && runid.dir == 1 && runid.seg == 85) {
			if (real_toll_notification_rates.containsKey(min)) {
				int count = real_toll_notification_rates.get(min);
				real_toll_notification_rates.put(min, count+1);
			} else {
				real_toll_notification_rates.put(min,1);
		}}	
	}
	
	/**
	 * Zero toll notification rate is maintained for only one run with identifier 0,1,85.  
	 * @param runid
	 * @param minute
	 */
	public void update_zero_tollnotification_rates (RunID runid, double min) {
		if (runid.xway == 0 && runid.dir == 1 && runid.seg == 85) {
			if (zero_toll_notification_rates.containsKey(min)) {
				int count = zero_toll_notification_rates.get(min);
				zero_toll_notification_rates.put(min, count+1);
			} else {
				zero_toll_notification_rates.put(min,1);
		}}	
	}
	
	/**
	 * Accident warning rate is maintained only for one run with identifier 0,1,85.  
	 * @param runid
	 * @param minute
	 */
	public void update_accidentwarning_rates (RunID runid, double min) {
		if (runid.xway == 0 && runid.dir == 1 && runid.seg == 85) {
			if (accident_warning_rates.containsKey(min)) {
				int count = accident_warning_rates.get(min);
				accident_warning_rates.put(min, count+1);
			} else {
				accident_warning_rates.put(min,1);
		}}	
	}
	
	/**
	 * Rates of events of each event type are written into separate files. This method is called for only one run with identifier 0,1,85.  
	 * @param pr_file stores rates of position reports
	 * @param tn_file stores rates of toll notifications
	 * @param aw_file stores rates of accident warnings
	 * @param lastSec is last second of simulation
	 */
	public void writeStreamRates2File (BufferedWriter pr_file, BufferedWriter rtn_file, BufferedWriter ztn_file, BufferedWriter aw_file, int lastMin) {
		
		try {
			for (double min=0; min<=lastMin; min++) {
				
				if (position_report_rates.containsKey(min)) pr_file.write(Math.round(min) + " " + position_report_rates.get(min) + "\n");
				if (real_toll_notification_rates.containsKey(min)) rtn_file.write(Math.round(min) + " " + real_toll_notification_rates.get(min) + "\n");
				if (zero_toll_notification_rates.containsKey(min)) ztn_file.write(Math.round(min) + " " + zero_toll_notification_rates.get(min) + "\n");
				if (accident_warning_rates.containsKey(min)) aw_file.write(Math.round(min) + " " + accident_warning_rates.get(min) + "\n");  
			}
		} catch (IOException e) { e.printStackTrace(); }
	}
	
	/**
	 * Write the total number of input and output events and 
	 * max number of stored events to the given file.  
	 * @param file
	 */
	public void writeEventCounts2File (int seg, BufferedWriter pr_count_file, BufferedWriter max_num_stored_events_file, BufferedWriter rtn_count_file, BufferedWriter ztn_count_file, BufferedWriter aw_count_file) {
		try {
			pr_count_file.write(seg + " " + position_reports_count + "\n");  
			max_num_stored_events_file.write(seg + " " +  maxLengthOfEventQueue + "\n"); 
			if (real_toll_count > 0) rtn_count_file.write(seg + " " + real_toll_count + "\n"); 
			if (zero_toll_count > 0) ztn_count_file.write(seg + " " + zero_toll_count + "\n");
			if (accidentWarnings.size() > 0) aw_count_file.write(seg + " " + accidentWarnings.size() + "\n"); 
		} catch (IOException e) { e.printStackTrace(); }
	}
	
	/**
	 * Write the toll notifications to the given file.  
	 * @param file
	 */
	public double writeTollNotifications2File (BufferedWriter file, double max_latency) {
		for (TollNotification t : tollNotifications) {
			try { 
				if (max_latency < t.totalProcessingTime) max_latency = t.totalProcessingTime;
				sum += t.totalProcessingTime;
				count++;
				file.write(t.toString()); 
			} catch (IOException e) { e.printStackTrace(); }
		}  
		return max_latency;
	}
	
	/**
	 * Write the accident warnings to the given file. 
	 * @param file
	 */
	public double writeAccidentWarnings2File (BufferedWriter file, double max_latency) {
		for (AccidentWarning a : accidentWarnings) {			
			try { 
				if (max_latency < a.totalProcessingTime) max_latency = a.totalProcessingTime;
				sum += a.totalProcessingTime;
				count++;
				file.write(a.toString()); 
			} catch (IOException e) { e.printStackTrace(); }
		}
		return max_latency;
	}
}
