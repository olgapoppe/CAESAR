package distributor;

import java.util.ArrayList;
import event.PositionReport;

public class EventQueue {
	
	final ArrayList<PositionReport> contents;
	public boolean shutdown;
		
	public EventQueue() {
		contents = new ArrayList<PositionReport>();
		shutdown = false;
	}
	
	public synchronized void put (ArrayList<PositionReport> batch) {
		try {
			while(!contents.isEmpty() && !shutdown) {
				wait();
			}
		} catch (InterruptedException e) { e.printStackTrace(); }
		
		contents.addAll(batch);
		notifyAll();		
	}
	
	public synchronized ArrayList<PositionReport> get() {
		try {
			while (contents.isEmpty() && !shutdown) {
				wait();
			} 
		} catch (InterruptedException e) { e.printStackTrace(); }
			
		ArrayList<PositionReport> batch = new ArrayList<PositionReport>();
		batch.addAll(contents);
		
		contents.clear();
		notifyAll();
			
		return batch;		
	}
	
	public synchronized void shutdown() {
		shutdown = true;
		notifyAll();
	}
}
