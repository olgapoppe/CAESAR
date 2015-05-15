package distributor;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import driver.EventQueue;
import run.*;

/**
 * Event distributor reads the input stream and distributes the events into the run task queues.
 * It notifies the scheduler when all events with the same time stamp were distributed.
 * @author Olga Poppe
 */
public abstract class EventDistributor implements Runnable {
	
	AtomicInteger distributorProgress;
	EventQueue events;
	HashMap<RunID,Run> runs;
	final RunQueues runqueues;
			
	public int min_stream_rate;
	public int max_stream_rate;
	
	AtomicInteger xway0dir0firstHPseg;
	AtomicInteger xway0dir1firstHPseg;
	int lastSec;
	long startOfSimulation;
		
	public EventDistributor (AtomicInteger dp, EventQueue e, HashMap<RunID,Run> rs, RunQueues rq, AtomicInteger x1, AtomicInteger x2, int last, long start) {
		
		distributorProgress = dp;
		events = e;
		runs = rs;
		runqueues = rq;	
				
		min_stream_rate = Integer.MAX_VALUE;
		max_stream_rate = 0;
		
		xway0dir0firstHPseg = x1;
		xway0dir1firstHPseg = x2;
		lastSec = last;
		startOfSimulation = start;
	}
}