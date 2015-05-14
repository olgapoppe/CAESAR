package distributor;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import run.RunID;
import event.PositionReport;

public class RunQueues {
	
	public HashMap<RunID,LinkedBlockingQueue<PositionReport>> contents;
	AtomicInteger distributorProgress;
		
	public RunQueues (AtomicInteger dp) {
		
		contents = new HashMap <RunID,LinkedBlockingQueue<PositionReport>>();
		distributorProgress = dp;
	}
	
	public synchronized void setDistributorProgress (Double d) {
		
		distributorProgress.set(d.intValue());
		
		System.out.println("Distributor: " + d);
		
		notifyAll();
	}

	public synchronized boolean getDistributorProgress (double sec) {	
		
		try {
			while (distributorProgress.get() < sec) {
				wait();
			} 
		} catch (InterruptedException e) { e.printStackTrace(); }
			
		return true;		
	}
}
