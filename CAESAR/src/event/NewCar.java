package event;

public class NewCar extends PositionReport {
	
	public NewCar (double type, double sec, double min, double vid, double spd, double xway, double lane, double dir, double seg, double pos) {
		super(type, sec, min, vid, spd, xway, lane, dir, seg, pos);
	}
}
