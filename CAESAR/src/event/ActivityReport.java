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
	public static ActivityReport parse_original_file (String line) {
		
		String[] values = line.split(" ");
		
		double new_sec = Double.parseDouble(values[0]);
        double new_min = Math.floor(new_sec/60) + 1;
        
        double new_act = Double.parseDouble(values[1]);
        String new_hr_string = values[2];
        double new_hr = new_hr_string.equals("NaN") ? -1 : Double.parseDouble(new_hr_string);    	    	    	
        ActivityReport event = new ActivityReport(new_sec, new_min, new_act, new_hr); 
        
    	//System.out.println(event.toString());    	
        return event;
	}
	
	/**
	 * Parse the given line and construct an activity report.
	 * @param line	
	 * @return activity report
	 */
	public static ActivityReport parse (String line) {
		
		String[] values = line.split(",");
		
		double new_sec = Double.parseDouble(values[0]);
        double new_min = Math.floor(new_sec/60) + 1;
        
        double new_pid = Double.parseDouble(values[1]);
        double new_act = Double.parseDouble(values[2]);
        double new_hr = Double.parseDouble(values[3]);
        ActivityReport event = new ActivityReport(new_sec, new_min, new_pid, new_act, new_hr);        	
        
    	//System.out.println(event.toString());    	
        return event;
	}
	
	/** 
	 * Print this activity report to file and change the person identifier to the given value.
	 * @param new person identifier	 * 
	 */
	public String toStringChangePid (int pid) {
		return 	new Double(sec).intValue() + ","				
				+ pid + ","
				+ new Double(activity).intValue() + ","
				+ new Double(heartRate).intValue();		
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
