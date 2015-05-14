package scheduler;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import distributor.*;
import event.*;
import run.*;

/**
 * As soon as all events with the same time stamp become available,
 * run driven scheduler submits only HP runs for execution during an accident.
 * Otherwise, all runs are processed in time-driven manner.
 * @author Olga Poppe 
 */
public class RunDrivenScheduler extends Scheduler implements Runnable {
	
	AtomicInteger xway0dir0HPrunsFromSeg;
	AtomicInteger xway0dir1HPrunsFromSeg;
	int HPrun_frequency;
	int LPrun_frequency;
	
	RunDrivenScheduler (AtomicInteger dp, HashMap<RunID,Run> rs, RunQueues rq, ExecutorService e, 
						CountDownLatch tn, CountDownLatch d, int last, long start, 
						AtomicInteger x0, AtomicInteger x1, int hprf, int lprf) {		
		super(dp,rs,rq,e,tn,d,last,start);
		xway0dir0HPrunsFromSeg = x0;
		xway0dir1HPrunsFromSeg = x1;
		HPrun_frequency = hprf;
		LPrun_frequency = lprf;
	}
	
	/**
	 * As long as not all events are processed, iterate over all run task queues and pick tasks to execute.
	 * If there is an accident, HP runs are executed faster. Otherwise, all runs are processed in time-driven manner.
	 */	
	public void run() {	
		
		/*** Local variables ***/
		double hp_sec = -1;
		double lp_sec = -1;
		int number = 0;
		int hp_count = 0;
		int lp_count = 0;
		boolean LPruns_catchup = false;
				
		//while (!shutdown) {
			try {				 
				/*** If events with new time stamp are available, schedule their processing after the previous transactions are acknowledged ***/
				int distr_progr = distributorProgress.get();
				
				if (distr_progr > hp_sec) { 
					
					int x0 = xway0dir0HPrunsFromSeg.get();
					int x1 = xway0dir1HPrunsFromSeg.get();
					boolean accident = (x0>=0 || x1>=0); 
										
					/*** If there is an accident, HP runs are processed faster ***/
					if (accident) {
						
						if (hp_count < HPrun_frequency && lp_count < LPrun_frequency && hp_sec > lp_sec) {
							
							hp_sec++;
							lp_sec = hp_sec;
							hp_count++;
							lp_count++;	
							number = all_queries_all_runs(hp_sec, true, true);	// 2 waitings
							
							//System.out.println("All queries all runs: " + number);
							
						} else {
						if (hp_count < HPrun_frequency) {
								
							hp_sec++;
							hp_count++;					
							number = all_queries_HP_runs (hp_sec, true, x0, x1, false);	// 2 waitings
							
							//System.out.println("All queries HP runs: " + number);
						}
						if (hp_count == HPrun_frequency && lp_count == LPrun_frequency) {
								
							hp_count = 0;
							lp_count = 0;
						}}							
						LPruns_catchup = true;
						
					} else {
					/*** If there is no accident, all runs are processed in time-driven manner. Previous LP runs catch up with previous HP runs in one transaction. ***/
						
						hp_sec++;
						lp_sec = hp_sec;
						
						if (LPruns_catchup) {							
							
							number = all_queries_all_runs (lp_sec, true, true); // 2 waitings
							LPruns_catchup = false;
							
						} else {
						
							number = all_queries_all_runs (lp_sec, true, false); // 2 waitings
					}}														
				} else {
				/*** If the stream is over, catch up LP runs if any, wait for acknowledgment of the previous transactions and sleep. ***/
					
					if (hp_sec == lastSec) {
						
						System.out.println("HP runs are done. LP runs are processed from second " + lp_sec);
						
						while (lp_sec < lastSec) { // Never called if the last accident is cleared before the end of the stream
							
							lp_sec++;
							number = all_queries_all_runs (lp_sec, true, false); // 2 waitings !!! No need to iterate over HP runs !!!						
						}						
						transaction_number.await(); 						
						done.countDown();												
					}					
					//Thread.sleep(getSleepTime(number));					
				}
			} catch (final InterruptedException ex) { ex.printStackTrace(); }
		//}	
	}
}
