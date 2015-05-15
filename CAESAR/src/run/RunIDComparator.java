package run;

import java.util.Comparator;

/**
 * Event comparator determines the position of a run identifier in an ordered data structure.
 * Run identifiers are ordered by their hash codes. 
 * 
 * @author olga
 */
public class RunIDComparator implements Comparator<RunID> {

	public int compare (RunID r1, RunID r2) {
		if (r1.hashCode() == r2.hashCode()) {
			return 0;
		} else {
			return (r1.hashCode() > r2.hashCode())? 1 : -1;   
		}
	}
}
