package distributor;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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
		
		Double now = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
		distributorProgressPerSec.put(sec, now);
		
		//System.out.println("SET: " + sec);
		distributorProgress.set(sec.intValue());		
		
		notifyAll();
	}

	public synchronized double getDistributorProgress (double sec, long startOfSimulation) {	
		
		double diff = 0;
		
		while (distributorProgress.get() < sec) {
				
			try { wait(); } catch (InterruptedException e) { e.printStackTrace(); }			
				
			if (distributorProgressPerSec.containsKey(sec)) {
				
				//System.out.println("GET: " + sec);				
				double availabilityTime = distributorProgressPerSec.get(sec);
				double now = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
				diff = now - availabilityTime;				
				if (diff > 1) System.out.println("Scheduler waited for distributor " + diff + " seconds too long.");
			}
		}			
		return diff;		
	}
}
