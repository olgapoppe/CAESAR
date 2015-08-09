package distributor;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import run.RunID;
import event.PositionReport;

public class EventQueues {
	
	public HashMap<RunID,ConcurrentLinkedQueue<PositionReport>> contents;
	AtomicInteger distributorProgress;
	HashMap<Double,Double> distributorProgressPerSec;
			
	public EventQueues (AtomicInteger dp) {
		
		contents = new HashMap <RunID,ConcurrentLinkedQueue<PositionReport>>();
		distributorProgress = dp;
		distributorProgressPerSec = new HashMap<Double,Double>();
	}
	
	public synchronized void setDistributorProgress (Double sec, long startOfSimulation) {
		
		distributorProgress.set(sec.intValue());
		
		Double now = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
		distributorProgressPerSec.put(sec, now);
		
		notifyAll();
	}

	public synchronized boolean getDistributorProgress (double sec, long startOfSimulation, AtomicBoolean accidentWarningsFailed, AtomicBoolean tollNotificationsFailed) {	
		
		try {
			while (distributorProgress.get() < sec) {
				
				wait();				
				
				if (sec > 50) {
					double availabilityTime = distributorProgressPerSec.get(sec);
					double now = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
					double diff = now - availabilityTime;				
					if (diff > 1) System.out.println("Scheduler waited for distributor " + diff + " seconds too long.");
				}
			} 
		} catch (InterruptedException e) { e.printStackTrace(); }
			
		return true;		
	}
}
