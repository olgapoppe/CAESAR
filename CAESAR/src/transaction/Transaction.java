package transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

import run.*;
import event.*;

/** 
 * A transaction has a time stamp, a run and a sequence of events 
 * that are executed by the run. 
 * @author Olga Poppe
 */
public abstract class Transaction implements Runnable {
	
	public Run run;
	ArrayList<PositionReport> events;
		
	HashMap<RunID,Run> runs;
	long startOfSimulation;
	HashMap<Double,Long> distributorProgressPerSec;
	
	public CountDownLatch transaction_number;
			
	public Transaction (Run r, ArrayList<PositionReport> eventList, HashMap<RunID,Run> rs, long start, HashMap<Double,Long> distrProgrPerSec) {
		
		run = r;	
		events = eventList;
		
		runs = rs;
		startOfSimulation = start;		
		distributorProgressPerSec = distrProgrPerSec;
	}	
}
