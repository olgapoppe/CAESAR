package distributor;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import event.PositionReport;

public class EventQueue {
	
	final LinkedBlockingQueue<PositionReport> contents;
	AtomicInteger driverProgress;
	public boolean shutdown;
		
	public EventQueue(AtomicInteger dp) {
		driverProgress = dp;
		contents = new LinkedBlockingQueue<PositionReport>();
		shutdown = false;
	}
	
	public synchronized void set (Double d) {
		
		driverProgress.set(d.intValue());
		
		System.out.println("-----------------------\nDriver: " + d);
		
		notifyAll();		
	}
	
	public synchronized boolean get (double sec) {
		try {
			
			while (driverProgress.get() < sec && !shutdown) {
				wait();
			} 
		} catch (InterruptedException e) { e.printStackTrace(); }
			
		return true;		
	}
	
	public synchronized void shutdown() {
		shutdown = true;
		notifyAll();
	}
}
