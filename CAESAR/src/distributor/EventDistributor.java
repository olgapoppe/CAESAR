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
	
	AtomicInteger distributorProgress;
		
	String filename;
	HashMap<RunID,Run> runs;
	final EventQueues eventqueues;
			
	AtomicInteger xway0dir0firstHPseg;
	AtomicInteger xway0dir1firstHPseg;
	int lastSec;
	long startOfSimulation;
	boolean count_and_rate;
		
	public EventDistributor (AtomicInteger dp, String f, HashMap<RunID,Run> rs, EventQueues rq, 
			AtomicInteger x1, AtomicInteger x2, int last, long start, boolean cr) {
		
		distributorProgress = dp;
				
		filename = f;
		runs = rs;
		eventqueues = rq;	
				
		xway0dir0firstHPseg = x1;
		xway0dir1firstHPseg = x2;
		lastSec = last;
		startOfSimulation = start;
		count_and_rate = cr;
	}
}