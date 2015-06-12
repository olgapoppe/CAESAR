package distributor;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import run.RunID;
import event.PositionReport;

public class RunQueues {
	
	public HashMap<RunID,LinkedBlockingQueue<PositionReport>> contents;
	AtomicInteger distributorProgress;
	int sec;
		
	public RunQueues (AtomicInteger dp) {
		
		contents = new HashMap <RunID,LinkedBlockingQueue<PositionReport>>();
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

	public synchronized boolean getDistributorProgress (double sec, AtomicBoolean accidentWarningsFailed, AtomicBoolean tollNotificationsFailed) {	
		
		try {
			while (distributorProgress.get() < sec) {
				long startOfWaiting = System.currentTimeMillis();
				wait();
				long durationOfWaiting = System.currentTimeMillis() - startOfWaiting;
				if (accidentWarningsFailed.get() || tollNotificationsFailed.get()) 
					System.out.println(sec + ": Scheduler waiting time for distributor is " + durationOfWaiting);
			} 
		} catch (InterruptedException e) { e.printStackTrace(); }
			
		return true;		
	}
}
