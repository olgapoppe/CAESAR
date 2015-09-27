package event;

/**
 * In addition to type, time stamp and person identifier, 
 * an activity report has minute, activity and heart rate. 
 * @author Olga Poppe
 */
public class ActivityReport extends Event {
	
	public double min;
	public double activity;
	public double heartRate;
	
	public double distributorTime;
	public double schedulerTime;
		
	public ActivityReport (double sec, double m, double a, double h) {
		super(0, sec, 0);	
		min = m;		
		activity = a;
		heartRate = h;			
	}
	
	public ActivityReport (double sec, double m, double pid, double a, double h) {
		super(0, sec, pid);	
		min = m;		
		activity = a;
		heartRate = h;			
	}
	
	/**
	 * Parse the given line and construct an activity report.
	 * @param line	
	 * @return activity report
	 */
	public static PositionReport parse (String line, int pid) {
		
		String[] values = line.split(" ");
		
		double new_sec = Double.parseDouble(values[0]);
        double new_min = Math.floor(new_sec/60) + 1;
        
        double new_act = Double.parseDouble(values[1]);
        String new_hr_string = values[2];
        double new_hr = new_hr_string.equals("NaN") ? 0 : Double.parseDouble(new_hr_string);    	    	    	
        PositionReport event = new PositionReport(0, new_sec, new_min, pid, new_hr, pid, 0, pid, pid, new_act); 
        
    	//System.out.println(event.toString());    	
        return event;
	}	
	
	/** 
	 * Print this activity report.
	 */
	public String toString() {
		return "type: " + type + 
				" sec: " + sec + 
				" pid: " + id + 
				" activity: " + activity + 
				" heartRate: " + heartRate;
	}	
}
