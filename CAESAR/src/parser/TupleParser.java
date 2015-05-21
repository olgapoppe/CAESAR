package parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import event.PositionReport;

public class TupleParser {
	
	public static HashMap<Integer,ArrayList<PositionReport>> parseTuples (String filename, int lastSec) {
		
		HashMap<Integer,ArrayList<PositionReport>> all_events = new HashMap<Integer,ArrayList<PositionReport>>();
		
		Scanner scanner;
		try {
			// Input file
			scanner = new Scanner(new File(filename));
			
			// Time
			Integer sec = 0;	
			//int count = 0;
			
			// First event
			String line = scanner.nextLine();
	 		PositionReport event = PositionReport.parse(line);	
	 								
			while (sec <= lastSec) {
				
				ArrayList<PositionReport> events = new ArrayList<PositionReport>();
				//count = 0;
				
				// Put events with time stamp sec into the list and reset the input event	 		
		 		while (event != null && event.sec == sec) {		 			
		 					 			
		 			events.add(event);	
		 			//count++;
		 					 			
		 			// Reset event
		 			if (scanner.hasNextLine()) {		 				
		 				line = scanner.nextLine();   
		 				event = PositionReport.parse(line);		 				
		 			} else {
		 				event = null;		 				
		 			}
		 		}
		 		all_events.put(sec, events);		 		
		 		//System.out.println(count + " events during second " + sec + " were parsed.");		 		
		 		sec++;
			}			
			/*** Clean-up ***/		
			scanner.close();				
			System.out.println("Parser is done.");
		}
		catch (FileNotFoundException e) { e.printStackTrace(); }	
		
		return all_events;
	}
}
