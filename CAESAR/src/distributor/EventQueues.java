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
	int sec;
		
	public EventQueues (AtomicInteger dp) {
		
		contents = new HashMap <RunID,ConcurrentLinkedQueue<PositionReport>>();
		distributorProgress = dp;
		sec = 0;
	}
	
	public synchronized void setDistributorProgress (Double d) {
		
		distributorProgress.set(d.intValue());
		
		/*// Output the current progress every 5 min
		if (d == sec+300) {
			System.out.println("Distributor: " + d);
			sec += 300;
		}*/			
		notifyAll();
	}

	public synchronized boolean getDistributorProgress (double sec, long startOfSimulation, AtomicBoolean accidentWarningsFailed, AtomicBoolean tollNotificationsFailed) {	
		
		try {
			while (distributorProgress.get() < sec) {
				//long startOfWaiting = System.currentTimeMillis();
				//long startOfScheduler = startOfWaiting - startOfSimulation;
				wait();
				//long durationOfWaiting = System.currentTimeMillis() - startOfWaiting;
				//if (accidentWarningsFailed.get() || tollNotificationsFailed.get()) 
					//System.out.println(sec + ": Scheduler started at " + startOfScheduler + " and waited for distributor " + durationOfWaiting + "ms");
			} 
		} catch (InterruptedException e) { e.printStackTrace(); }
			
		return true;		
	}
}
