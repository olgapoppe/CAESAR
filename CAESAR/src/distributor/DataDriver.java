package distributor;

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
		
	public DataDriver(AtomicInteger dp, String f, EventQueue e, double s) {
		
		driverProgress = dp;
		filename = f;
		events = e;
		lastSec = s;
	}
	
	public void run() {
		
		Scanner scanner;
		try {
			// Input file
			scanner = new Scanner(new File(filename));
			
			// Current second
			Double curr_sec = new Double(-1);
			double dp = -1;
						
			// Output file
			//File input_file = new File("../../input_till_sec_10784.dat");
			//BufferedWriter input = new BufferedWriter(new FileWriter(input_file));	
			
			// First event
			String line = scanner.nextLine();
	 		PositionReport event = PositionReport.parse(line);	
						
			while (curr_sec <= lastSec) {
				
				/*************************************** Event number ***************************************/
				Random random = new Random();
				int min = 6;
				int max = 14;	
				int number = random.nextInt(max - min + 1) + min;
				
				// Arrival time
				curr_sec = (curr_sec==-1) ? number : curr_sec+number;	
							
				/****************************************** Event batch *******************************************/		 		
		 		while (event != null && event.sec <= curr_sec) {
		 			
		 			// Write the event to the output file and append its arrival time
		 			event.arrivalTime = curr_sec;
		 			events.contents.add(event);
		 			
		 			dp = event.sec;
		 			
		 			// Reset event
		 			if (scanner.hasNextLine()) {		 				
		 				line = scanner.nextLine();   
		 				event = PositionReport.parse(line);	
		 			} else {
		 				event = null;		 				
		 			}
		 		}	
		 		/****************************************** Driver progress *******************************************/
		 		events.set(dp);		 		
		 		
				Thread.sleep(number * 1000);
			}			
			/*** Clean-up ***/		
			scanner.close();	
			
			System.out.println("driver done");
		}
		catch (InterruptedException e) { e.printStackTrace(); }
		catch (FileNotFoundException e1) { e1.printStackTrace(); }		
	}
}
