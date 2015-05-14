package distributor;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import event.PositionReport;

public class EventQueue {
	
	final LinkedBlockingQueue<PositionReport> contents;
	AtomicInteger driverProgress;
	int sec;
			
	public EventQueue(AtomicInteger dp) {
		
		driverProgress = dp;
		contents = new LinkedBlockingQueue<PositionReport>();	
		sec = 0;
	}
	
	public synchronized void setDriverPrgress (Double d) {
		
		driverProgress.set(d.intValue());
		
		// Output the current progress every 5 min
		if (d == sec+300) {
			System.out.println("-----------------------\nDriver: " + d);
			sec += 300;
		}		
		notifyAll();		
	}
	
	public synchronized boolean getDriverProgress (double sec) {
		try {			
			while (driverProgress.get() < sec) {
				wait();
			} 
		} catch (InterruptedException e) { e.printStackTrace(); }
			
		return true;		
	}
}
