package window;

import java.util.ArrayList;
import java.util.Collections;

public class WindowDistribution {
	
	/** @param args: 
	 * 				OUTPUT TYPE
	 * 0			output_type: 0 for uniform distribution, 1 for Poisson distribution, 2 for shared overlapping windows
	 * 
	 * 				INPUT DATA
	 * 1			last second : 10784
	 *  
	 * 				CONTEXT WINDOWS
	 * 2			window center (sec)
	 * 3			window length (sec)
	 * 4			window number
	 * 5			query number 
	 * 
	 * 				OVERLAPPING CONTEXT WINDOWS
	 * 6			shared : 1 for yes, 0 for no
	 * 7			overlap length (sec)
	 * 8			group number of overlapping windows
	 * 9			group size 
	 */	
	public static void main (String args[]) {
		
		/*** Validate the number of input parameters ***/
	    if (args.length < 10) {
			System.out.println("At least 10 input parameters are expected.");
			return;
		}
		
		/*** Read input parameters ***/	
	    int output_type =  Integer.parseInt(args[0]);
	        
		int lastSec = Integer.parseInt(args[1]);
		
		int window_center = Integer.parseInt(args[2]);
		int window_length = Integer.parseInt(args[3]);
		int window_number = Integer.parseInt(args[4]);
		int query_number = Integer.parseInt(args[5]);
		
		boolean shared = args[6].equals("1");
		int overlap_length = Integer.parseInt(args[7]);
		int group_number = Integer.parseInt(args[8]);
		int group_size = Integer.parseInt(args[9]);
		
		if (output_type == 0) {			
		
			/*** Uniform window distribution ***/			
			System.out.println("Window distribution: uniform" +
								"\nWindow length: " + window_length + 
								"\nWindow number: " + window_number);
			
			ArrayList<TimeInterval> timeIntervals = getTimeIntervalsForUniformDistribution(lastSec, window_length, window_number);
			int count = 1;
			for (TimeInterval i : timeIntervals) {
				String s = i.toString();
				if (count % 5 == 0) s+="\n";
				System.out.print(s);
				count++;
			}
		} else { 
		if (output_type == 1) {
		
			/*** Poisson window distribution ***/
			int lambda = window_center/window_length + 1;
			
			System.out.println("Window distribution: Poisson" +
								"\nWindow length: " + window_length + 
								"\nWindow number: " + window_number +
								"\nCenter: " + window_center + 
								"\nLambda: " + lambda);				
			
			int count_1 = 0;
			while (count_1 < 5) {
				System.out.println(getTimeIntervalsForPoissonDistribution(lastSec, window_length, window_number, lambda));
				count_1++;
			}
		} else {
		
			/*** Shared overlapping windows ***/
			System.out.println(	"Shared overlapping windows: " + shared +
								"\nWindow length: " + window_length + 
								"\nWindow overlap length: " + overlap_length +
								"\nOverlapping window group number: " + group_number +
								"\nWindow group size: " + group_size);
			
			ArrayList<TimeInterval> timeIntervals = getTimeIntervalsForSharedWindows(shared, lastSec, window_length, query_number, overlap_length, group_number, group_size);
			int count = 1;
			for (TimeInterval i : timeIntervals) {
				String s = i.toString();
				if (count % 3 == 0) s+="\n";
				System.out.print(s);
				count++;
			}		
		}}
	}
	
	public static ArrayList<TimeInterval> getTimeIntervalsForPoissonDistribution (double lastSec, int window_length, int window_number, int lambda) {
		
		/*** Get expensive windows and sort them ***/
		ArrayList<Integer> expensive_windows = getPoissonNumbers(lastSec, window_length, window_number, lambda);
		Collections.sort(expensive_windows);
		//System.out.println(expensive_windows);
		
		/*** Get expensive time intervals ***/
		ArrayList<TimeInterval> results = new ArrayList<TimeInterval>();
		for (Integer expensive_window : expensive_windows) {
			
			double start = new Double(expensive_window * window_length + 1).intValue();
			double end = new Double((expensive_window + 1) * window_length).intValue();
			
			TimeInterval i = new TimeInterval(start,end);
			results.add(i);			
		}			
		return results;
	}
	
	public static ArrayList<Integer> getPoissonNumbers (double lastSec, int window_length, int window_number, double lambda) {
		
		ArrayList<Integer> results = new ArrayList<Integer> ();
		int total_window_number = new Double(lastSec).intValue()/window_length - 1;
			
		while (results.size() < window_number) {
			
			int result = getPoisson(lambda);
			if (!results.contains(result) && result <= total_window_number) results.add(result);
		}
		return results;
	}
	
	public static int getPoisson (double lambda) {
		
		double L = Math.exp(-lambda);
		double p = 1.0;
		int k = 0;

		do {
			k++;
		    p *= Math.random();
		} while (p > L);

		return k - 1;
	}
	
	public static ArrayList<TimeInterval> getTimeIntervalsForUniformDistribution (double lastSec, int window_length, int window_number) {
		 	
		/*** Get cheap window length ***/
		double expensive_time = window_number * window_length;
		double cheap_time = lastSec - expensive_time;
		double cheap_window_length_double = cheap_time/(window_number + 1); 
		int cheap_window_length = new Double(cheap_window_length_double).intValue();
		if (cheap_window_length_double - cheap_window_length > 0.5) cheap_window_length++;
		
		/*** Get expensive time intervals ***/
		ArrayList<TimeInterval> results = new ArrayList<TimeInterval>();
		double window_bound = cheap_window_length;
		while (results.size() < window_number) {
			
			double start = new Double(window_bound).intValue();
			double end = new Double(window_bound + window_length - 1).intValue();
			
			if (start < 0 || end > lastSec) {
				System.err.println("Data bounds are exceeded by the time interval [" + start + "," + end + "]!");
				break;
			} else {
				TimeInterval i = new TimeInterval(start,end);
				results.add(i);
			}		
			window_bound = end + 1 + cheap_window_length;
		}			
		return results;
	}
	
	public static ArrayList<TimeInterval> getTimeIntervalsForSharedWindows (boolean shared, double lastSec, int window_length, int query_number,
			int overlap_length, int group_number, int group_size) {
		
		ArrayList<TimeInterval> results = new ArrayList<TimeInterval>();
		
		ArrayList<TimeInterval> first_expensive_windows_per_group = WindowDistribution.getTimeIntervalsForUniformDistribution(lastSec, window_length, group_number);
		
		/*** Windows overlap completely ***/
		if (window_length == overlap_length) {
			
			int shared_query_number = (shared) ? query_number : (group_size * query_number);
			for (TimeInterval first : first_expensive_windows_per_group) {
				TimeInterval shared_window = new TimeInterval(first.start, first.end, shared_query_number);
				results.add(shared_window);
			}			
		} else { /*** Windows do not overlap completely ***/
			
			for (TimeInterval first : first_expensive_windows_per_group) {
			
				double first_start = first.start;
				double first_end = first.end - overlap_length;
				int less_shared_query_number = (shared) ? query_number : (group_size/2 * query_number);
				TimeInterval first_window = new TimeInterval (first_start, first_end, less_shared_query_number);
				results.add(first_window);
			
				double shared_start = first_end + 1;
				double shared_end = first.end;
				int shared_query_number = (shared) ? query_number : (group_size * query_number); 
				TimeInterval shared_window = new TimeInterval (shared_start, shared_end, shared_query_number);
				results.add(shared_window);
			
				double last_start = shared_end + 1;
				double last_end = shared_start + window_length - 1;
				TimeInterval last_window = new TimeInterval (last_start, last_end, less_shared_query_number);
				results.add(last_window);
		}}	
		return results;
	}
}
