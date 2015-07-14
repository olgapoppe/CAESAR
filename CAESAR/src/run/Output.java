package run;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

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
	
	public void update_positionreport_rates (double sec) {
		if (position_report_rates.containsKey(sec)) {
			int count = position_report_rates.get(sec);
			position_report_rates.put(sec, count+1);
		} else {
			position_report_rates.put(sec,1);
		}	
	}
	
	public void update_tollnotification_rates (double sec) {
		if (toll_notification_rates.containsKey(sec)) {
			int count = toll_notification_rates.get(sec);
			toll_notification_rates.put(sec, count+1);
		} else {
			toll_notification_rates.put(sec,1);
		}	
	}
	
	public void update_accidentwarning_rates (double sec) {
		if (accident_warning_rates.containsKey(sec)) {
			int count = accident_warning_rates.get(sec);
			accident_warning_rates.put(sec, count+1);
		} else {
			accident_warning_rates.put(sec,1);
		}	
	}
	
	public void writeStreamRates2File (RunID runid, BufferedWriter file, int lastSec) {
		
		try {
			for (double sec=0; sec<=lastSec; sec++) {
				
				int position_report_count = position_report_rates.containsKey(sec) ? position_report_rates.get(sec) : 0;	
				int toll_notification_count = toll_notification_rates.containsKey(sec) ? toll_notification_rates.get(sec) : 0;
				int accident_warning_count = accident_warning_rates.containsKey(sec) ? accident_warning_rates.get(sec) : 0;
				file.write(runid.toString() + " " + 
						Math.round(sec) + " " +
						position_report_count + " " +
						toll_notification_count + " " +
						accident_warning_count + "\n");   
			}
		} catch (IOException e) { e.printStackTrace(); }
	}
	
	
	
	/**
	 * Write the total number of input and output events and 
	 * max number of stored events to the given file.  
	 * @param file
	 */
	public void writeEventCounts2File (RunID runid, BufferedWriter file) {
		try {
			file.write(runid.toString() + " " + 
					position_reports_count + " " + 
					maxLengthOfEventQueue + " " +
					tollNotifications.size() + " " + 
					accidentWarnings.size() + "\n");   
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
