package iogenerator;

public class XwayDirPair {
	
	public int xway;
	public int dir;
	
	XwayDirPair (int x, int d) {
		xway = x;
		dir = d;
	}
	
	public String toString() {
		return "xway: " + xway + " dir: " + dir;
	}
}
