package run;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import event.*;

/** 
 * Every run maintains its output consisting of processed position reports, generated toll notifications and accident warnings,
 * number of processed events and maximal number of stored events.
 * 
 * @author olga
 */
public class Output {
	
	public ArrayList<PositionReport> positionReports;
	public ArrayList<TollNotification> tollNotifications;
	public ArrayList<AccidentWarning> accidentWarnings;
	
	public int numberOfProcessedEvents;
	public int maxNumberOfStoredEvents;
	
	public Output () {
		
		positionReports  = new ArrayList<PositionReport>();
		tollNotifications  = new ArrayList<TollNotification>();
		accidentWarnings = new ArrayList<AccidentWarning>();
		
		numberOfProcessedEvents = 0;
		maxNumberOfStoredEvents = 0;
	}
	
	/**
	 * Write the processing times of position reports to the given file. 
	 * 
	 * @param file
	 */	
	public void write2FileEventProcessingTimes (BufferedWriter file) {
		for (PositionReport p : positionReports) {
			p.write2FileEventProcessingTime(file);
		}  
	}
	
	/**
	 * Write the toll notifications to the given file. 
	 * 
	 * @param file
	 */
	public void write2FileTollNotifications (BufferedWriter file) {
		for (TollNotification t : tollNotifications) {
			try { file.write(t.toString()); } catch (IOException e) { e.printStackTrace(); }
		}  
	}
	
	/**
	 * Write the accident warnings to the given file. 
	 * 
	 * @param file
	 */
	public void write2FileAccidentWarnings (BufferedWriter file) {
		for (AccidentWarning a : accidentWarnings) {			
			try { file.write(a.toString()); } catch (IOException e) { e.printStackTrace(); }
		}  
	}
}
