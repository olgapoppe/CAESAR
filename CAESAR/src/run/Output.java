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
	
	HashMap<Double,Integer> position_report_rates;
	HashMap<Double,Integer> toll_notification_rates;
	HashMap<Double,Integer> accident_warning_rates;
	
	public Output () {
		
		//positionReports  = new ArrayList<PositionReport>();
		tollNotifications  = new ArrayList<TollNotification>();
		accidentWarnings = new ArrayList<AccidentWarning>();
		
		position_reports_count = 0;
		maxLengthOfEventQueue = 0;
		
		position_report_rates = new HashMap<Double,Integer>();
		toll_notification_rates = new HashMap<Double,Integer>();
		accident_warning_rates = new HashMap<Double,Integer>();
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
	 * @param second
	 */	
	public void update_positionreport_rates (RunID runid, double sec) {
		if (runid.xway == 0 && runid.dir == 1 && runid.seg == 85) {
			if (position_report_rates.containsKey(sec)) {
				int count = position_report_rates.get(sec);
				position_report_rates.put(sec, count+1);
			} else {
				position_report_rates.put(sec,1);
		}}
	}
	
	/**
	 * Toll notification rate is maintained for only one run with identifier 0,1,85.  
	 * @param runid
	 * @param second
	 */
	public void update_tollnotification_rates (RunID runid, double sec) {
		if (runid.xway == 0 && runid.dir == 1 && runid.seg == 85) {
			if (toll_notification_rates.containsKey(sec)) {
				int count = toll_notification_rates.get(sec);
				toll_notification_rates.put(sec, count+1);
			} else {
				toll_notification_rates.put(sec,1);
		}}	
	}
	
	/**
	 * Accident warning rate is maintained only for one run with identifier 0,1,85.  
	 * @param runid
	 * @param second
	 */
	public void update_accidentwarning_rates (RunID runid, double sec) {
		if (runid.xway == 0 && runid.dir == 1 && runid.seg == 85) {
			if (accident_warning_rates.containsKey(sec)) {
				int count = accident_warning_rates.get(sec);
				accident_warning_rates.put(sec, count+1);
			} else {
				accident_warning_rates.put(sec,1);
		}}	
	}
	
	/**
	 * Rates of events of each event type are written into separate files. This method is called for only one run with identifier 0,1,85.  
	 * @param pr_file stores rates of position reports
	 * @param tn_file stores rates of toll notifications
	 * @param aw_file stores rates of accident warnings
	 * @param lastSec is last second of simulation
	 */
	public void writeStreamRates2File (BufferedWriter pr_file, BufferedWriter tn_file, BufferedWriter aw_file, int lastSec) {
		
		try {
			for (double sec=0; sec<=lastSec; sec++) {
				
				if (position_report_rates.containsKey(sec)) pr_file.write(Math.round(sec) + " " + position_report_rates.get(sec) + "\n");
				if (toll_notification_rates.containsKey(sec)) tn_file.write(Math.round(sec) + " " + toll_notification_rates.get(sec) + "\n");
				if (accident_warning_rates.containsKey(sec)) aw_file.write(Math.round(sec) + " " + accident_warning_rates.get(sec) + "\n");  
			}
		} catch (IOException e) { e.printStackTrace(); }
	}
	
	/**
	 * Write the total number of input and output events and 
	 * max number of stored events to the given file.  
	 * @param file
	 */
	public void writeEventCounts2File (int seg, BufferedWriter pr_count_file, BufferedWriter max_num_stored_events_file, BufferedWriter tn_count_file, BufferedWriter aw_count_file) {
		try {
			pr_count_file.write(seg + " " + position_reports_count + "\n");  
			max_num_stored_events_file.write(seg + " " +  maxLengthOfEventQueue + "\n"); 
			if (tollNotifications.size() > 0) tn_count_file.write(seg + " " + tollNotifications.size() + "\n"); 
			if (accidentWarnings.size() > 0) aw_count_file.write(seg + " " + accidentWarnings.size() + "\n"); 
		} catch (IOException e) { e.printStackTrace(); }
	}
	
	/**
	 * Write the toll notifications to the given file.  
	 * @param file
	 */
	public void writeTollNotifications2File (BufferedWriter file) {
		for (TollNotification t : tollNotifications) {
			try { file.write(t.toString()); } catch (IOException e) { e.printStackTrace(); }
		}  
	}
	
	/**
	 * Write the accident warnings to the given file. 
	 * @param file
	 */
	public void writeAccidentWarnings2File (BufferedWriter file) {
		for (AccidentWarning a : accidentWarnings) {			
			try { file.write(a.toString()); } catch (IOException e) { e.printStackTrace(); }
		}  
	}
}
