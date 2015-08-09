package distributor;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import run.RunID;
import event.PositionReport;

public class EventQueues {
	
	public HashMap<RunID,ConcurrentLinkedQueue<PositionReport>> contents;
	AtomicInteger distributorProgress;
				
	public EventQueues (AtomicInteger dp) {
		
		contents = new HashMap <RunID,ConcurrentLinkedQueue<PositionReport>>();
		distributorProgress = dp;		
	}
	
	public synchronized void setDistributorProgress (Double sec, long startOfSimulation) {
		
		distributorProgress.set(sec.intValue());			
		notifyAll();		
	}

	public synchronized boolean getDistributorProgress (double sec, long startOfSimulation) {	
		
		try {			
			while (distributorProgress.get() < sec) {				
				wait(); 						
			}	
		} catch (InterruptedException e) { e.printStackTrace(); }
		return true;		
	}
}
