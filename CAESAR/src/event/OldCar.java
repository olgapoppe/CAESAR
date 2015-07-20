package event;

public class OldCar extends PositionReport {
	
	public OldCar (PositionReport e) {
		super(e.type, e.sec, e.min, e.vid, e.spd, e.xway, e.lane, e.dir, e.seg, e.pos);
	}
}
