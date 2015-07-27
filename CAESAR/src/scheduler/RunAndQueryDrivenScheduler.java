package scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import run.*;
import distributor.*;
import iogenerator.*;

/**
 * As soon as all events with the same time stamp become available,
 * run and query driven scheduler submits HP runs and HP queries for execution more often than LP runs and LP queries.
 * @author Olga Poppe 
 */
public class RunAndQueryDrivenScheduler extends Scheduler implements Runnable {

	public final RunQueues HPrunqueues;
	AtomicInteger xway0dir0HPrunsFromSeg;
	AtomicInteger xway0dir1HPrunsFromSeg;
	int HP_frequency;
	int LP_frequency;
	
	public RunAndQueryDrivenScheduler (AtomicInteger dp, HashMap<Double,Long> distrProgrPerSec, HashMap<RunID,Run> rs, RunQueues rq, RunQueues hprq,ExecutorService e, 
								CountDownLatch tn, CountDownLatch d, ArrayList<XwayDirPair> xds, int lastS, long start, AtomicInteger x0, AtomicInteger x1, int hpf, int lpf) {		
		super(dp,distrProgrPerSec,rs,rq,e,tn,d,xds,lastS,start);
		HPrunqueues = hprq;
		xway0dir0HPrunsFromSeg = x0;
		xway0dir1HPrunsFromSeg = x1;
		HP_frequency = hpf;
		LP_frequency = lpf;
	}
	
	/**
	 * As long as not all events are processed, iterate over all run task queues and pick tasks to execute.
	 * If there is an accident, HP runs and HP queries of LP runs are executed faster than LP queries of LP runs.
	 * Otherwise, HP queries are processed faster than LP queries.
	 */
	public void run() {	
		
		/*** Local variables ***/
		double hp_sec = -1;
		double lp_sec = -1;
		int hp_count = 0;
		int lp_count = 0;
		boolean LPqueries_of_LPruns_catchup = false;
				
		//while (!shutdown) {
			try {				 
				/*** If events with new time stamp are available, schedule their processing after the previous transactions are acknowledged ***/
				int distr_progr = distributorProgress.get();
				
				if (distr_progr > hp_sec) { 
					
					int x0 = xway0dir0HPrunsFromSeg.get();
					int x1 = xway0dir1HPrunsFromSeg.get();
					boolean accident = (x0>=0 || x1>=0); 
										
					/*** If there is an accident, HP runs and HP queries of LP runs are processed faster than LP queries of LP runs ***/
					if (accident) {						
						if (hp_count < HP_frequency && lp_count < LP_frequency) {
							
							hp_sec++;
							hp_count++;
							lp_sec++;
							lp_count++;
							// all queries in all runs
						}
						if (hp_count < HP_frequency) {
							
							hp_sec++;
							hp_count++;
							// all queries in HP runs and HP queries in LP runs (HP computations)
						} 										
						if (hp_count == HP_frequency && lp_count == LP_frequency) {
							
							hp_count = 0;
							lp_count = 0;
						}
						LPqueries_of_LPruns_catchup = true;
					} else {
					/*** If there is no accident, HP queries are processed faster than LP queries. LP queries of previous LP runs catch up with previous HP computations in one transaction. ***/
						
						hp_sec++;
						lp_sec = hp_sec;
						
						if (LPqueries_of_LPruns_catchup) {							
							
							hp_count = 0;
							lp_count = 0;
							LPqueries_of_LPruns_catchup = false;
							// LP queries of LP runs catch up with HP computations							
							
						} else {
						
							if (hp_count < HP_frequency && lp_count < LP_frequency) {
								
								hp_sec++;
								hp_count++;
								lp_sec++;
								lp_count++;
								// all queries in all runs
							}
							if (hp_count < HP_frequency) {
								
								hp_sec++;
								hp_count++;
								// HP queries in all runs
							} 										
							if (hp_count == HP_frequency && lp_count == LP_frequency) {
								
								hp_count = 0;
								lp_count = 0;
							} 
						}										
					}												
				} else {
					/*** If the stream is over, catch up LP runs if any, wait for acknowledgment of the previous transactions and sleep. ***/
					
					if (hp_sec == lastSec) {
						
						System.out.println("HP runs and HP queries are done. LP queries of LP runs are processed from second " + lp_sec);
						
						while (lp_sec < lastSec) { // Never called if the last accident is cleared before the end of the stream
							
							lp_sec++;
							// LP queries in LP runs													
						}						
						transaction_number.await(); 						
						done.countDown();												
					}					
					//Thread.sleep(sleepTime); 					
				}
			} catch (final InterruptedException ex) { ex.printStackTrace(); }
		//}	
	}
}
