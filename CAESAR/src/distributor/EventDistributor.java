package distributor;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import run.*;

/**
 * Event distributor reads the input stream and distributes the events into the run task queues.
 * It notifies the scheduler when all events with the same time stamp were distributed.
 * @author Olga Poppe
 */
public abstract class EventDistributor implements Runnable {
	
	String filename;
	double lastSec;
	
	HashMap<RunID,Run> runs;
	final EventQueues eventqueues;			
	
	long startOfSimulation;
	AtomicInteger distributorProgress;
	HashMap<Double,Double> distrFinishTimes;
	
	boolean count_and_rate;
		
	public EventDistributor (String f, double last, 
			HashMap<RunID,Run> rs, EventQueues rq, 
			long start, AtomicInteger dp, HashMap<Double,Double> dFTimes,
			boolean cr) {
		
		filename = f;
		lastSec = last;
		
		runs = rs;
		eventqueues = rq;					
		
		startOfSimulation = start;
		distributorProgress = dp;
		distrFinishTimes = dFTimes;
		
		count_and_rate = cr;
	}
}