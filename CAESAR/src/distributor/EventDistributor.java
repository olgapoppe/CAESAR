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
	HashMap<Double,Long> distributorProgressPerSec;
	
	String filename;
	HashMap<RunID,Run> runs;
	final RunQueues runqueues;
			
	AtomicInteger xway0dir0firstHPseg;
	AtomicInteger xway0dir1firstHPseg;
	int lastSec;
	long startOfSimulation;
		
	public EventDistributor (AtomicInteger dp, HashMap<Double,Long> distrProgrPerSec, String f, HashMap<RunID,Run> rs, RunQueues rq, AtomicInteger x1, AtomicInteger x2, int last, long start) {
		
		distributorProgress = dp;
		distributorProgressPerSec = distrProgrPerSec;
		
		filename = f;
		runs = rs;
		runqueues = rq;	
				
		xway0dir0firstHPseg = x1;
		xway0dir1firstHPseg = x2;
		lastSec = last;
		startOfSimulation = start;
	}
}