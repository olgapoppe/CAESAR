package window;

import java.util.ArrayList;

public class WindowDistribution {
	
	public static void main (String args[]) {
		
		int lambda = 10;
		int expensive_window_number = 5;
		
		int lastSec = 20;
		int window_length = 2;	
		
		/* int count = 0;
		 * while (count < 100) {
			System.out.println(expensive_window_number + " poisson numbers with lambda " + lambda + " are: " + getPoissonNumbers(lastSec,window_length,lambda,expensive_window_number));
			count++;
		}*/
		
		System.out.println(expensive_window_number + " uniform numbers are: " + getUniformNumbers(lastSec,window_length,expensive_window_number) +
				"\nTheir respective time intervals are: " + getTimeIntervals(0,lastSec,window_length,lambda,expensive_window_number).toString());
	}
	
	public static ArrayList<TimeInterval> getTimeIntervals (int distribution, double lastSec, int window_length, int lambda, int expensive_window_number) {
		
		ArrayList<TimeInterval> results = new ArrayList<TimeInterval>();
		
		ArrayList<Integer> expensive_windows = (distribution == 0) ? 
				getUniformNumbers(lastSec,window_length,expensive_window_number) :
				getPoissonNumbers(lastSec,window_length,lambda,expensive_window_number);
		
		for (Integer expensive_window : expensive_windows) {
			
			double start = expensive_window * window_length + 1;
			double end = (expensive_window + 1) * window_length;
			
			TimeInterval i = new TimeInterval(start,end);
			results.add(i);			
		}		
		return results;
	}
	
	public static ArrayList<Integer> getPoissonNumbers (double lastSec, int window_length, int expensive_window_number, double lambda) {
		
		ArrayList<Integer> results = new ArrayList<Integer> ();
		double total_window_number = lastSec/window_length;
			
		while (results.size() < expensive_window_number) {
			
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
	
	public static ArrayList<Integer> getUniformNumbers (double lastSec, int window_length, int expensive_window_number) {
		  
		ArrayList<Integer> results = new ArrayList<Integer>();
		
		/*** Get total window number ***/
		int total_window_number = new Double(lastSec).intValue()/window_length;
		// Not enough windows
		if (total_window_number < expensive_window_number) {
			System.err.println(	"Total window number is: " + total_window_number + 
								" Expensive window number: " + expensive_window_number + "\n");			
		} else {
			// Exact number of windows
			if (total_window_number == expensive_window_number) {
				int result = 0;		
				while (result < expensive_window_number) {
					results.add(result);
					result++;
				}
			} else {			
				int cheap_window_number_inbetween = total_window_number/(expensive_window_number+1) - 1;				
				if (cheap_window_number_inbetween == 0) {					
					if (expensive_window_number*2-1 > total_window_number) {
						// Without cheap windows in-between
						int count = 0;
						int result = 0;		
						while (count < expensive_window_number) {
							results.add(result);
							result++;
							count++;
						}
					} else {
						// With 1 cheap window in-between
						int count = 0;
						int result = 0;		
						while (count < expensive_window_number) {			
							results.add(result);
							result += 2;
							count++;
						}
					}
				} else {
					// With cheap windows in-between
					int current_window = cheap_window_number_inbetween;
					
					int count = 0;		
					while (count < expensive_window_number) {	
						int result = current_window + 1;
						results.add(result);
						current_window = result + cheap_window_number_inbetween;
						count++;
					}
				}
			}
		}			
		return results;
	}
}
