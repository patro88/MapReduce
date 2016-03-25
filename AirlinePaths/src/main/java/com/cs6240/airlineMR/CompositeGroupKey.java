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

public class CompositeGroupKey implements WritableComparable<CompositeGroupKey> {
	String carrier;
	String year;

	public CompositeGroupKey() {

	}

	public CompositeGroupKey(String name, String year) {
		this.carrier = name;
		this.year = year;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(carrier);
		out.writeUTF(year);
	}

	public void readFields(DataInput in) throws IOException {
		carrier = in.readUTF();
		year = in.readUTF();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((carrier == null) ? 0 : carrier.hashCode());
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
		CompositeGroupKey other = (CompositeGroupKey) obj;
		if (carrier == null) {
			if (other.carrier != null)
				return false;
		} else if (!carrier.equals(other.carrier))
			return false;
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}

	@Override
	public int compareTo(CompositeGroupKey t) {
		int cmp = this.carrier.compareTo(t.carrier);
		if (cmp != 0) {
			return cmp;
		}
		return this.year.compareTo(t.year);
	}
	
	@Override
	public String toString() {
		return carrier + "\t" + year ;
	}


}
