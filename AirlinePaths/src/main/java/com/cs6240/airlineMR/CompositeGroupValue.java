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
import org.apache.hadoop.io.WritableUtils;

public class CompositeGroupValue implements WritableComparable<CompositeGroupValue> {
	String carrier;
	String year;
	String cancelled;
	Long connections=0L;
	Long missedConnections=0L;
	Long actual=0L;
	Long scheduled=0L;
	String isArriving;
	String airport;
	
	
	public CompositeGroupValue() {}

	public CompositeGroupValue(String carrier, String year, String cancelled, Long connections, Long missedConnections,
			Long actual, Long scheduled, String isArrivinFlight, String airport) {
		this.connections = connections;
		this.missedConnections = missedConnections;
		this.actual = actual;
		this.scheduled = scheduled;
		this.isArriving = isArrivinFlight;
		this.airport = airport;
		this.carrier = carrier;
		this.year = year;
		this.cancelled = cancelled;
	}
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, carrier);
		WritableUtils.writeString(out, year);
		WritableUtils.writeString(out, cancelled);
		WritableUtils.writeVLong(out, connections);
		WritableUtils.writeVLong(out, missedConnections);
		WritableUtils.writeVLong(out, actual);
		WritableUtils.writeVLong(out, scheduled);
		WritableUtils.writeString(out, isArriving);
		WritableUtils.writeString(out, airport);
	}

	public void readFields(DataInput in) throws IOException {
		carrier = WritableUtils.readString(in);
		year = WritableUtils.readString(in);
		cancelled = WritableUtils.readString(in);
		connections = WritableUtils.readVLong(in);
		missedConnections = WritableUtils.readVLong(in);
		actual = WritableUtils.readVLong(in);
		scheduled = WritableUtils.readVLong(in);
		isArriving = WritableUtils.readString(in);
		airport = WritableUtils.readString(in);
	}
	
	@Override
	public int compareTo(CompositeGroupValue o) {
		int cmp = this.carrier.compareTo(o.carrier);
		if (cmp != 0) {
			return cmp;
		}
		cmp = this.year.compareTo(o.year);
		if (cmp != 0) {
			return cmp;
		}
		cmp = this.airport.compareTo(o.airport);
		if (cmp != 0) {
			return cmp;
		}
		cmp = this.scheduled.compareTo(o.scheduled);
		if (cmp != 0) {
			return cmp;
		}
		cmp = this.actual.compareTo(o.actual);
		if (cmp != 0) {
			return cmp;
		}
		cmp = this.cancelled.compareTo(o.cancelled);
		if (cmp != 0) {
			return cmp;
		}
		cmp = this.isArriving.compareTo(o.isArriving);
		if (cmp != 0) {
			return cmp;
		}
		cmp = this.connections.compareTo(o.connections);
		if (cmp != 0) {
			return cmp;
		}
		return this.missedConnections.compareTo(o.missedConnections);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((actual == null) ? 0 : actual.hashCode());
		result = prime * result + ((airport == null) ? 0 : airport.hashCode());
		result = prime * result
				+ ((cancelled == null) ? 0 : cancelled.hashCode());
		result = prime * result + ((carrier == null) ? 0 : carrier.hashCode());
		result = prime * result
				+ ((connections == null) ? 0 : connections.hashCode());
		result = prime * result
				+ ((isArriving == null) ? 0 : isArriving.hashCode());
		result = prime
				* result
				+ ((missedConnections == null) ? 0 : missedConnections
						.hashCode());
		result = prime * result
				+ ((scheduled == null) ? 0 : scheduled.hashCode());
		result = prime * result + ((year == null) ? 0 : year.hashCode());
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
		if (actual == null) {
			if (other.actual != null)
				return false;
		} else if (!actual.equals(other.actual))
			return false;
		if (airport == null) {
			if (other.airport != null)
				return false;
		} else if (!airport.equals(other.airport))
			return false;
		if (cancelled == null) {
			if (other.cancelled != null)
				return false;
		} else if (!cancelled.equals(other.cancelled))
			return false;
		if (carrier == null) {
			if (other.carrier != null)
				return false;
		} else if (!carrier.equals(other.carrier))
			return false;
		if (connections == null) {
			if (other.connections != null)
				return false;
		} else if (!connections.equals(other.connections))
			return false;
		if (isArriving == null) {
			if (other.isArriving != null)
				return false;
		} else if (!isArriving.equals(other.isArriving))
			return false;
		if (missedConnections == null) {
			if (other.missedConnections != null)
				return false;
		} else if (!missedConnections.equals(other.missedConnections))
			return false;
		if (scheduled == null) {
			if (other.scheduled != null)
				return false;
		} else if (!scheduled.equals(other.scheduled))
			return false;
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return carrier + "\t" + year
				+ "\t" + cancelled + "\t" + connections
				+ "\t" + missedConnections + "\t"
				+ actual + "\t" + scheduled + "\t"
				+ isArriving + "\t" + airport;
	}
	
	
}
