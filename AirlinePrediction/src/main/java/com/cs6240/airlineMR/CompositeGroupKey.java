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
	String year;
	String month;

	public CompositeGroupKey() {

	}

	public CompositeGroupKey(String year, String month) {
		this.year = year;
		this.month = month;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(year);
		out.writeUTF(month);
	}

	public void readFields(DataInput in) throws IOException {
		year = in.readUTF();
		month = in.readUTF();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((year == null) ? 0 : year.hashCode());
		result = prime * result + ((month == null) ? 0 : month.hashCode());
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
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		if (month == null) {
			if (other.month != null)
				return false;
		} else if (!month.equals(other.month))
			return false;
		return true;
	}

	@Override
	public int compareTo(CompositeGroupKey t) {
		int cmp = this.year.compareTo(t.year);
		if (cmp != 0) {
			return cmp;
		}
		return this.month.compareTo(t.month);
	}
	
	@Override
	public String toString() {
		return year + "\t" + month ;
	}


}
