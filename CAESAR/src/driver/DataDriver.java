package driver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import event.PositionReport;

public class DataDriver implements Runnable {
	
	AtomicInteger driverProgress;
	HashMap<Integer,ArrayList<PositionReport>> input;
	EventQueue events;
	
	double lastSec;
	long startOfSimulation;
		
	public DataDriver(AtomicInteger dp, HashMap<Integer,ArrayList<PositionReport>> i, EventQueue e, double last, long start) {
		
		driverProgress = dp;
		input = i;
		events = e;
		
		lastSec = last;
		startOfSimulation = start;
	}
	
	public void run() {
		
		// Time
		double app_sec = 0;
		double curr_sec = 0;
																
		while (app_sec <= lastSec) {
				
			// Write all events with the application time app_sec to the event queue	
			Integer i = new Double(app_sec).intValue();
		 	ArrayList<PositionReport> list = input.get(i);
		 	events.contents.addAll(list);
		 	input.remove(app_sec);
		 	
			// Set driver progress to app_sec
			events.setDriverPrgress(app_sec);
			
			// Sleep if curr_sec is smaller than app_sec
			curr_sec = (System.currentTimeMillis() - startOfSimulation)/1000;
			
			if (curr_sec < app_sec && app_sec < lastSec) {
	 			
	 			int sleep_time = new Double(app_sec - curr_sec).intValue();
	 			
	 			//System.out.println("Driver sleeps " + sleep_time + " seconds.");
	 			
	 			try { Thread.sleep(sleep_time * 1000); } catch (InterruptedException e) { e.printStackTrace(); }
	 		}
	 		app_sec++;
		}
		System.out.println("Driver is done.");			
	}
}
