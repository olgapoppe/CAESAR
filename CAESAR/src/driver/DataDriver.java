package driver;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import event.PositionReport;

public class DataDriver implements Runnable {
	
	AtomicInteger driverProgress;
	String filename;
	EventQueue events;
	
	double lastSec;
	long startOfSimulation;
		
	public DataDriver(AtomicInteger dp, String f, EventQueue e, double last, long start) {
		
		driverProgress = dp;
		filename = f;
		events = e;
		
		lastSec = last;
		startOfSimulation = start;
	}
	
	public void run() {
		
		Scanner scanner;
		try {
			// Input file
			scanner = new Scanner(new File(filename));
			
			// Time
			double curr_app_sec = 0;
			double curr_sec = 0;
																
			// Output file
			//File input_file = new File("../../input_till_sec_10784.dat");
			//BufferedWriter input = new BufferedWriter(new FileWriter(input_file));	
			
			// First event
			String line = scanner.nextLine();
	 		PositionReport event = PositionReport.parse(line);	
	 								
			while (curr_app_sec <= lastSec) {
				
				// Put events with time stamp curr_app_sec into the event queue		 		
		 		while (event != null && event.sec == curr_app_sec) {
		 			
		 			// Write the event to the output file and append its driver time	
		 			event.driverTime = (System.currentTimeMillis() - startOfSimulation)/1000;
		 			events.contents.add(event);
		 			
		 			System.out.println(event.toString());
		 					 			
		 			// Reset event
		 			if (scanner.hasNextLine()) {		 				
		 				line = scanner.nextLine();   
		 				event = PositionReport.parse(line);		 				
		 			} else {
		 				event = null;		 				
		 			}
		 		}
		 		// Set driver progress to curr_app_sec
		 		events.setDriverPrgress(curr_app_sec);
		 		
		 		// Sleep if curr_sec is smaller than curr_app_sec
		 		curr_sec = (System.currentTimeMillis() - startOfSimulation)/1000;
		 		
		 		if (curr_sec < curr_app_sec && curr_app_sec < lastSec) {
		 			
		 			int sleep_time = new Double(curr_app_sec - curr_sec).intValue();
		 			
		 			//System.out.println("Driver sleeps " + sleep_time + " seconds.");
		 			
		 			Thread.sleep(sleep_time * 1000);
		 		}
		 		curr_app_sec++;
			}			
			/*** Clean-up ***/		
			scanner.close();				
			System.out.println("Driver is done.");
		}
		catch (InterruptedException e) { e.printStackTrace(); }
		catch (FileNotFoundException e1) { e1.printStackTrace(); }		
	}
}
