package distributor;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import run.*;
import event.PositionReport;

public class DoubleQueueDistributor extends EventDistributor {
	
	final HashMap<RunID,LinkedBlockingQueue<PositionReport>> HPruntaskqueues;
	
	public DoubleQueueDistributor (AtomicInteger dp, EventQueue e, HashMap<RunID,Run> rs, HashMap<RunID,LinkedBlockingQueue<PositionReport>> rtq, HashMap<RunID,LinkedBlockingQueue<PositionReport>> hprtq,
								   AtomicInteger x1, AtomicInteger x2) {
		super(dp, e, rs, rtq, x1, x2);
		HPruntaskqueues = hprtq;
	}

	/** 
	 * Read the input file, parse the events, 
	 * generate new runs if they do not exist yet and
	 * distribute events into run task queues.
	 */	
	public void run() {	
		
		/*Scanner scanner;
		try {
			*//*** Local variables ***//*
			scanner = new Scanner(new File(filename));
			double curr_min = 0;		
			Double curr_sec = new Double(-1);
			int event_count = 0;
					 			
		 	while (scanner.hasNextLine()) {
			
		 		*//******************************************* Event *******************************************//*
		 		String line = scanner.nextLine();   
		 		PositionReport event = PositionReport.parse(line);  
   			 	
		 		*//*** Current minute ***//*
		 		if (event.min > curr_min) {   			 		
		 			System.out.println("Minute: " + event.min);
		 			curr_min = event.min;  			 		   			 		
		 		}   	
		 		
		 		if (event.type == 0) {
		 			
		 			*//******************************************* Run *******************************************//*
		 			RunID runid = new RunID (event.xway, event.dir, event.seg); 
		 			Run run;        		
		 			if (runs.containsKey(runid)) {
		 				run = runs.get(runid);             			          			
		 			} else {
		 				AtomicInteger firstHPseg = (runid.dir == 0) ? xway0dir0firstHPseg : xway0dir1firstHPseg;
		 				run = new Run(runid, event.sec, event.min, firstHPseg);
		 				runs.put(runid, run);
		 			}  			 	
		 			*//************************************* Run task queues *************************************//*
		 			LinkedBlockingQueue<PositionReport> runtaskqueue = runtaskqueues.get(runid);
		 			if (runtaskqueue == null) {    
		 				runtaskqueue = new LinkedBlockingQueue<PositionReport>();
		 				runtaskqueues.put(runid, runtaskqueue);		 				
		 			}
		 			runtaskqueue.add(event);	 	
		 			LinkedBlockingQueue<PositionReport> HPruntaskqueue = HPruntaskqueues.get(runid);
		 			if (HPruntaskqueue == null) {    
		 				HPruntaskqueue = new LinkedBlockingQueue<PositionReport>();
		 				HPruntaskqueues.put(runid, HPruntaskqueue);		 				
		 			}
		 			HPruntaskqueue.add(event);
		 			
		 			*//*** Max number of stored events per run ***//*
		 			int size = runtaskqueue.size() + HPruntaskqueue.size();
		 			if (run.output.maxNumberOfStoredEvents < size) run.output.maxNumberOfStoredEvents = size;	 
		 			
		 			if (event.sec > curr_sec) {			 				
		 						 				
		 			*//********************************** Distributer progress **********************************//*
		 				distributorProgress.set(curr_sec.intValue());
		 							 					
		 				*//*** Min and max stream rate ***//*
		 				if (curr_sec >= 0) {
		 					if (min_stream_rate > event_count) min_stream_rate = event_count;
		 					if (max_stream_rate < event_count) max_stream_rate = event_count;
		 				}		 				
		 				curr_sec = event.sec;
		 				event_count = 1;
		 				
		 			} else { 
		 				event_count++;
		 			}	 			
		 	}}
		 	*//************************************** Last second **************************************//*
		 	distributorProgress.set(curr_sec.intValue());			
							
			*//*** Min and max stream rate ***//*
			if (min_stream_rate > event_count) min_stream_rate = event_count;
			if (max_stream_rate < event_count) max_stream_rate = event_count;
				
		 	*//*** Close scanner ***//*		
			scanner.close();			
		
		} catch (FileNotFoundException e1) { e1.printStackTrace(); } 	*/					 
	}
}
