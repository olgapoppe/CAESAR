package iogenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import parser.TupleParser;
import run.*;
import scheduler.*;
import distributor.*;
import driver.*;
import event.*;

public class EventPreprocessor {
	
	/*String filename;
	int scheduling_strategy; 
	HashMap<RunID,Run> runs;
	ExecutorService executor;
	
	CountDownLatch done;
	int lastXway;
	boolean lastXwayUnidir;
	int lastSec;
	int HP_frequency;
	int LP_frequency;
	
	EventPreprocessor(String f, int ss, HashMap<RunID,Run> rs, ExecutorService e, CountDownLatch d, int lX, boolean lXU, int lS, int HP_freq, int LP_freq) {
		
		filename = f;
		scheduling_strategy = ss; 
		runs = rs;
		executor = e;
		
		done = d;
		lastXway = lX;
		lastXwayUnidir = lXU;
		lastSec = lS;
		HP_frequency = HP_freq;
		LP_frequency = LP_freq;		
	}*/
	
	public static void preprocess (String filename, int scheduling_strategy, HashMap<RunID,Run> runs, ExecutorService executor, 
			CountDownLatch done, int lastXway, boolean lastXwayUnidir, int lastSec, int HP_frequency, int LP_frequency) {
		
		/*** Parse input tuples and store events in the hash table ***/
		HashMap<Integer,ArrayList<PositionReport>> input = TupleParser.parseTuples(filename, lastSec);				
		
		/*** Create shared data structures ***/
		AtomicInteger driverProgress = new AtomicInteger(-1);
		AtomicInteger distributorProgress = new AtomicInteger(-1);	
		
		EventQueue events = new EventQueue(driverProgress);					
		RunQueues runqueues = new RunQueues(distributorProgress);
		RunQueues HPrunqueues = new RunQueues(distributorProgress);	
		
		CountDownLatch transaction_number = new CountDownLatch(0);				
		AtomicInteger xway0dir0firstHPseg = new AtomicInteger(-1);
		AtomicInteger xway0dir1firstHPseg = new AtomicInteger(-1);	
		long startOfSimulation = System.currentTimeMillis();		
		
		/*** Create and start data driver ***/
		/*DataDriver dataDriver = new DataDriver(driverProgress, input, events, lastSec, startOfSimulation);
		Thread drThread = new Thread(dataDriver);
		drThread.setPriority(10);
		drThread.start();*/
		
		/*** Depending on scheduling strategy, create and start event distributing and query scheduling threads ***/
		EventDistributor distributor;
		Scheduler scheduler;
		
		if (scheduling_strategy < 3) {
			
			distributor = new SingleQueueDistributor(distributorProgress, events, runs, runqueues,
												xway0dir0firstHPseg, xway0dir1firstHPseg, lastSec, startOfSimulation);		
			
			if (scheduling_strategy == 1) {
			
				System.out.println("TIME DRIVEN SCHEDULER.");
				scheduler = new TimeDrivenScheduler(distributorProgress, runs, runqueues, executor, 
												transaction_number, done, lastXway, lastXwayUnidir, lastSec, startOfSimulation);
			} else {			
			
				System.out.println("RUN DRIVEN SCHEDULER.");
				scheduler = new RunDrivenScheduler(	distributorProgress, runs, runqueues, executor, 
												transaction_number, done, lastXway, lastXwayUnidir, lastSec, startOfSimulation, 
												xway0dir0firstHPseg, xway0dir1firstHPseg, HP_frequency, LP_frequency);
			}
		} else {
			
			distributor = new DoubleQueueDistributor(distributorProgress, events, runs, runqueues, HPrunqueues,
												xway0dir0firstHPseg, xway0dir1firstHPseg, lastSec, startOfSimulation);	
			
			if (scheduling_strategy == 3) {
			
				System.out.println("QUERY DRIVEN SCHEDULER.");
				scheduler = new QueryDrivenScheduler(distributorProgress, runs, runqueues, HPrunqueues, executor, 
												transaction_number, done, lastXway, lastXwayUnidir, lastSec, startOfSimulation,
												HP_frequency, LP_frequency);
			} else {
			
				System.out.println("RUN AND QUERY DRIVEN SCHEDULER.");
				scheduler = new RunAndQueryDrivenScheduler(distributorProgress, runs, runqueues, HPrunqueues, executor, 
												transaction_number, done, lastXway, lastXwayUnidir, lastSec, startOfSimulation,
												xway0dir0firstHPseg, xway0dir1firstHPseg, HP_frequency, LP_frequency);
		}}
		Thread prodThread = new Thread(distributor);
		prodThread.setPriority(10);
		prodThread.start();
		
		Thread consThread = new Thread(scheduler);
		consThread.setPriority(10);
		consThread.start();
	}
}
