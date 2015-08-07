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
				double startOfWaiting = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
				wait();
				double endOfWaiting = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
				double durationOfWaiting = endOfWaiting - startOfWaiting;
				if (durationOfWaiting>1 && sec>8000) 
					System.out.println(	"Scheduler waits from " + startOfWaiting + 
										" to " + endOfWaiting + 
										" for distributor to processes second " + sec);
			} 
		} catch (InterruptedException e) { e.printStackTrace(); }
			
		return true;		
	}
}
