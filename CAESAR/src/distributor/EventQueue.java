package distributor;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import event.PositionReport;

public class EventQueue {
	
	final LinkedBlockingQueue<PositionReport> contents;
	AtomicInteger driverProgress;
			
	public EventQueue(AtomicInteger dp) {
		driverProgress = dp;
		contents = new LinkedBlockingQueue<PositionReport>();		
	}
	
	public synchronized void setDriverPrgress (Double d) {
		
		driverProgress.set(d.intValue());
		
		System.out.println("-----------------------\nDriver: " + d);
		
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
