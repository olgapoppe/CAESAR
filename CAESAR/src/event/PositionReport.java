package event;

import java.io.BufferedWriter;
import java.io.IOException;

/**
 * In addition to type, time stamp and vehicle identifier, 
 * a position report has minute, speed, expressway, lane, direction, segment, position and processing time. 
 * @author Olga Poppe
 */
public class PositionReport extends Event {
	
	public double min;
	public double spd; 
	public double xway; 
	public double lane;
	public double dir; 
	public double seg;
	public double pos;	
	//public double processingTime;
	
	public double distributorTime;
	public double schedulerTime;
	public double executorTime;
	
	public PositionReport (double t, double sec, double m, double v, double s, double x, double l, double d, double s1, double p) {
		super(t, sec, v);	
		min = m;		
		spd = s;
		xway = x;
		lane = l;
		dir = d;
		seg = s1;
		pos = p;		
	}
	
	/**
	 * Parse the given line and construct a position report.
	 * @param line	
	 * @return position report
	 */
	public static PositionReport parse (String line) {
		
		String[] values = line.split(",");
		
		double new_type = Double.parseDouble(values[0]);
        double new_sec = Double.parseDouble(values[1]);
        double new_min = Math.floor(new_sec/60) + 1;
    	double new_vid = Double.parseDouble(values[2]);          	
    	double new_spd = Double.parseDouble(values[3]);
    	double new_xway = Double.parseDouble(values[4]);
    	double new_lane = Double.parseDouble(values[5]);
    	double new_dir = Double.parseDouble(values[6]);
    	double new_seg = Double.parseDouble(values[7]);
    	double new_pos = Double.parseDouble(values[8]);    
    	    	    	
    	PositionReport event = new PositionReport(new_type, new_sec, new_min, new_vid, new_spd, new_xway, new_lane, new_dir, new_seg, new_pos);    	
    	//System.out.println(event.toString());    	
        return event;
	}
	
	public boolean correctPositionReport() {
		return type==0 && sec>=0 && vid>=0 && spd>=0 && xway>=0 && lane>=0 && dir>=0 && seg>=0 && pos>=0;
	}
	
	/**
	 * Determine whether this position report is equal to the given position report.
	 * 
	 * @param e	position report
	 * @return boolean
	 */	
	public boolean equals (PositionReport e) {
		return 	type == e.type &&	
				sec == e.sec &&				
				vid == e.vid &&
				spd == e.spd &&
				xway == e.xway &&
				lane == e.lane &&
				dir == e.dir &&
				seg == e.seg &&
				pos == e.pos;
	}	
	
	/**
	 * Write the application and processing time stamps of this position report to the given file. 
	 * @param file
	 */
	/*public void write2FileEventProcessingTime (BufferedWriter file) {
		
		int appTime = new Double(sec).intValue();
		int procTime = new Double(processingTime).intValue();
		
		String line = appTime + " " + procTime  + "\n"; 
		
		try { file.write(line); } catch (IOException e) { e.printStackTrace(); }
	}*/
	
	/** 
	 * Print this position report to file and change the xway to the given value.
	 * @param new xway
	 */
	public String toString(int newXway) {
		return new Double(type).intValue() + ","
				+ new Double(sec).intValue() + ","				
				+ new Double(vid).intValue() + ","
				+ new Double(spd).intValue() + ","
				+ newXway + ","
				+ new Double(lane).intValue() + ","
				+ new Double(dir).intValue() + ","
				+ new Double(seg).intValue() + ","
				+ new Double(pos).intValue();		
	}	
	
	/** 
	 * Print this position report.
	 */
	public String toString() {
		return "type: " + type + 
				" sec: " + sec + 
				" vid: " + vid + 
				" spd: " + spd + 
				" xway: " + xway + 
				" lane: " + lane + 
				" dir: " + dir + 
				" seg: " + seg +
				" pos: " + pos;
	}	
	
	/** 
	 * Print all time stamps of this position report.
	 */
	public String timesToString() {
		return 	"application time: " + sec +				
				"   distributor time: " + distributorTime + 
				"   scheduler time: " + schedulerTime + 
				"   executor time: " + executorTime;
	}	
}