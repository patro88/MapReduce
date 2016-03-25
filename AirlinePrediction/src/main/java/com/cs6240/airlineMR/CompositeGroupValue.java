package com.cs6240.airlineMR;

/*
 * @author: Shakti Patro
 * @maintainer : Kavyashree Nagendrakumar
 * 
 * Composite key Class.
 * Contains carrier and year as composite key. 
 * It extends the Writable comparable class and overrides the write, read, and compareTo methods
 *  
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author Shakti
 *
 */
public class CompositeGroupValue implements WritableComparable<CompositeGroupValue> {
	String carrier; 
	String origin ;
	String destination ;
	String flightNumber;
	String flightDate;
	String dayOfMonth ;
	String dayOfWeek ;
	String hourOfDay ;
	String crsDepTime ;
	String crsElapsedTime; 
	String distance ;
	String daysTillNearestHoliday; 
	String delay;
	
	public CompositeGroupValue() {

	}

	

	public CompositeGroupValue(String carrier, String origin,String flightDate,
			String destination, String flightNumber, String dayOfMonth,
			String dayOfWeek, String hourOfDay, String crsDepTime,
			String crsElapsedTime, String distance,
			String daysTillNearestHoliday, String delay) {
		super();
		this.carrier = carrier;
		this.origin = origin;
		this.flightDate = flightDate;
		this.destination = destination;
		this.flightNumber = flightNumber;
		this.dayOfMonth = dayOfMonth;
		this.dayOfWeek = dayOfWeek;
		this.hourOfDay = hourOfDay;
		this.crsDepTime = crsDepTime;
		this.crsElapsedTime = crsElapsedTime;
		this.distance = distance;
		this.daysTillNearestHoliday = daysTillNearestHoliday;
		this.delay = delay;
	}



	public void write(DataOutput out) throws IOException {
		out.writeUTF(carrier);
		out.writeUTF(origin );
		out.writeUTF(flightDate);
		out.writeUTF(destination );
		out.writeUTF(flightNumber); 
		out.writeUTF(dayOfMonth );
		out.writeUTF(dayOfWeek );
		out.writeUTF(hourOfDay );
		out.writeUTF(crsDepTime );
		out.writeUTF(crsElapsedTime); 
		out.writeUTF(distance );
		out.writeUTF(daysTillNearestHoliday); 
		out.writeUTF(delay);
	}

	public void readFields(DataInput in) throws IOException {
		carrier = in.readUTF();
		origin = in.readUTF();
		flightDate = in.readUTF();
		destination = in.readUTF();
		flightNumber = in.readUTF();
		dayOfMonth = in.readUTF();
		dayOfWeek = in.readUTF();
		hourOfDay = in.readUTF();
		crsDepTime = in.readUTF();
		crsElapsedTime = in.readUTF();
		daysTillNearestHoliday = in.readUTF();
		delay = in.readUTF();
		distance = in.readUTF();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((carrier == null) ? 0 : carrier.hashCode());
		result = prime * result
				+ ((crsDepTime == null) ? 0 : crsDepTime.hashCode());
		result = prime * result
				+ ((crsElapsedTime == null) ? 0 : crsElapsedTime.hashCode());
		result = prime * result
				+ ((dayOfMonth == null) ? 0 : dayOfMonth.hashCode());
		result = prime * result
				+ ((dayOfWeek == null) ? 0 : dayOfWeek.hashCode());
		result = prime
				* result
				+ ((daysTillNearestHoliday == null) ? 0
						: daysTillNearestHoliday.hashCode());
		result = prime * result + ((delay == null) ? 0 : delay.hashCode());
		result = prime * result
				+ ((destination == null) ? 0 : destination.hashCode());
		result = prime * result
				+ ((distance == null) ? 0 : distance.hashCode());
		result = prime * result
				+ ((flightDate == null) ? 0 : flightDate.hashCode());
		result = prime * result
				+ ((flightNumber == null) ? 0 : flightNumber.hashCode());
		result = prime * result
				+ ((hourOfDay == null) ? 0 : hourOfDay.hashCode());
		result = prime * result + ((origin == null) ? 0 : origin.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CompositeGroupValue other = (CompositeGroupValue) obj;
		if (carrier == null) {
			if (other.carrier != null)
				return false;
		} else if (!carrier.equals(other.carrier))
			return false;
		if (crsDepTime == null) {
			if (other.crsDepTime != null)
				return false;
		} else if (!crsDepTime.equals(other.crsDepTime))
			return false;
		if (crsElapsedTime == null) {
			if (other.crsElapsedTime != null)
				return false;
		} else if (!crsElapsedTime.equals(other.crsElapsedTime))
			return false;
		if (dayOfMonth == null) {
			if (other.dayOfMonth != null)
				return false;
		} else if (!dayOfMonth.equals(other.dayOfMonth))
			return false;
		if (dayOfWeek == null) {
			if (other.dayOfWeek != null)
				return false;
		} else if (!dayOfWeek.equals(other.dayOfWeek))
			return false;
		if (daysTillNearestHoliday == null) {
			if (other.daysTillNearestHoliday != null)
				return false;
		} else if (!daysTillNearestHoliday.equals(other.daysTillNearestHoliday))
			return false;
		if (delay == null) {
			if (other.delay != null)
				return false;
		} else if (!delay.equals(other.delay))
			return false;
		if (destination == null) {
			if (other.destination != null)
				return false;
		} else if (!destination.equals(other.destination))
			return false;
		if (distance == null) {
			if (other.distance != null)
				return false;
		} else if (!distance.equals(other.distance))
			return false;
		if (flightDate == null) {
			if (other.flightDate != null)
				return false;
		} else if (!flightDate.equals(other.flightDate))
			return false;
		if (flightNumber == null) {
			if (other.flightNumber != null)
				return false;
		} else if (!flightNumber.equals(other.flightNumber))
			return false;
		if (hourOfDay == null) {
			if (other.hourOfDay != null)
				return false;
		} else if (!hourOfDay.equals(other.hourOfDay))
			return false;
		if (origin == null) {
			if (other.origin != null)
				return false;
		} else if (!origin.equals(other.origin))
			return false;
		return true;
	}



	@Override
	public int compareTo(CompositeGroupValue o) {
		int cmp = this.carrier.compareTo(o.carrier);
		if (cmp != 0) {
			return cmp;
		}
		cmp = this.origin.compareTo(o.origin);
		if (cmp != 0) {
			return cmp;
		}
		cmp = this.flightDate.compareTo(o.flightDate);
		if (cmp != 0) {
			return cmp;
		}
		cmp = this.destination.compareTo(o.destination);
		if (cmp != 0) {
			return cmp;
		}
		cmp = this.flightNumber.compareTo(o.flightNumber);
		if (cmp != 0) {
			return cmp;
		}
		cmp = this.dayOfMonth.compareTo(o.dayOfMonth);
		if (cmp != 0) {
			return cmp;
		}
		cmp = this.dayOfWeek.compareTo(o.dayOfWeek);
		if (cmp != 0) {
			return cmp;
		}
		cmp = this.hourOfDay.compareTo(o.hourOfDay);
		if (cmp != 0) {
			return cmp;
		}
		cmp = this.crsDepTime.compareTo(o.crsDepTime);
		if (cmp != 0) {
			return cmp;
		}
		cmp = this.crsElapsedTime.compareTo(o.crsElapsedTime);
		if (cmp != 0) {
			return cmp;
		}
		cmp = this.distance.compareTo(o.distance);
		if (cmp != 0) {
			return cmp;
		}
		cmp = this.daysTillNearestHoliday.compareTo(o.daysTillNearestHoliday);
		if (cmp != 0) {
			return cmp;
		}
		return this.delay.compareTo(o.delay);
	}

	@Override
	public String toString() {
		return carrier + "\t" + origin
				+ "\t"  + destination + "\t" 
				+ flightNumber + "\t" + flightDate + "\t" + dayOfMonth + "\t" 
				+ dayOfWeek + "\t" + hourOfDay + "\t" 
				+ crsDepTime + "\t" + crsElapsedTime
				+ "\t" + distance + "\t" 
				+ daysTillNearestHoliday + "\t" + delay ;
	}
}
