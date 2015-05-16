package run;

/**
 * A run maintains several time stamps including the time stamps of the run progress,
 * run updates and total processing time due to overhead.
 * @author Olga Poppe
 */
public class Time {
	
	// Time stamp of the run progress
	public double sec;	
	public double min;
	
	// Time stamps of the updates
	public double minOfLastUpdateOfAccidentAhead;
	//public double minOfLastStorageOfEventProcessingTime;
	public double minOfLastGarbageCollection;
	
	// Overhead
	//public long garbageCollectionTime;
    //public long priorityMaintenanceTime;
    
    public Time (double s, double m) {
    	
    	sec = s;	
		min = m;
    	
    	minOfLastUpdateOfAccidentAhead = -1;
    	//minOfLastStorageOfEventProcessingTime = 0;
    	minOfLastGarbageCollection = 0;
    	
    	//garbageCollectionTime = 0;
    	//priorityMaintenanceTime = 0;
    }
}
