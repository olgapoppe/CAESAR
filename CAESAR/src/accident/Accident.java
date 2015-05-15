package accident;

import java.io.BufferedWriter;
import java.io.IOException;

import run.RunID;

/** 
 * Accident is described by run identifier (expressway, direction, segment), lane, position,
 * detection time and clearance time (both application and system time stamps are kept). 
 * @author Olga Poppe
 */

public class Accident {
	
	public RunID runID;
	
	public double lane;
	public double pos;
	
	public double detAppMin;
	public double clearAppMin;
	public double detProcTime;
	public double clearProcTime;	
	
	public Accident (RunID r, double l, double p, double dam, double dpt) {
		
		runID = r;
		
		lane = l;
		pos = p;
		
		detAppMin = dam;
		clearAppMin = -1;
		detProcTime = dpt;	
		clearProcTime = -1;
	}
	
	/**
	 * Write all information about this accident to the given file. 
	 * @param file
	 */	
	public void write2FileAccidentProcessingTime (BufferedWriter file) {
		
		int l = new Double(lane).intValue();
		int p = new Double(pos).intValue();
		int det = new Double(detProcTime).intValue();
		int cl = new Double(clearProcTime).intValue();
		
		String line = runID.toString() + " " + l + " " + p + " " + det + " " + cl  + "\n"; 
		
		try { file.write(line); } catch (IOException e) { e.printStackTrace(); }
	}
}
