package driver;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Random;
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
			Double curr_sec = new Double(-1);
			Double arrival_sec = new Double(-1);
														
			// Output file
			//File input_file = new File("../../input_till_sec_10784.dat");
			//BufferedWriter input = new BufferedWriter(new FileWriter(input_file));	
			
			// First event
			String line = scanner.nextLine();
	 		PositionReport event = PositionReport.parse(line);	
	 								
			while (curr_sec < lastSec) {
				
				/*************************************** Batch size ***************************************/
				Random random = new Random();
				int min = 6;
				int max = 14;	
				int batch_size = random.nextInt(max - min + 1) + min;
				
				// Time				 
				arrival_sec = (arrival_sec==-1) ? batch_size : arrival_sec+batch_size;	
							
				/****************************************** Event batch *******************************************/		 		
		 		while (event != null && event.sec <= arrival_sec) {
		 			
		 			// Write the event to the output file and append its arrival time
		 			event.arrivalTime = (System.currentTimeMillis() - startOfSimulation)/1000;		 			
		 			events.contents.add(event);
		 					 			
		 			// Set driver progress
		 			if (event.sec > curr_sec) { 
		 				events.setDriverPrgress(curr_sec);
		 				curr_sec++;
		 			}
		 			
		 			// Reset event
		 			if (scanner.hasNextLine()) {		 				
		 				line = scanner.nextLine();   
		 				event = PositionReport.parse(line);		 				
		 			} else {
		 				event = null;		 				
		 			}
		 		}	
		 		// Set driver progress to the time stamp of the last event in the batch and sleep
		 		events.setDriverPrgress(curr_sec);
		 		if (curr_sec < lastSec) Thread.sleep(batch_size * 1000);
			}			
			/*** Clean-up ***/		
			scanner.close();				
			System.out.println("Driver is done.");
		}
		catch (InterruptedException e) { e.printStackTrace(); }
		catch (FileNotFoundException e1) { e1.printStackTrace(); }		
	}
}
