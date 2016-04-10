package com.dsort;

import java.io.Serializable;

/**
 * @author Pankaj Tripathi 
 * @author Kartik Mahaley 
 * @author Shakti Patro 
 * @author Chen Bai
 * 
 * Functionality:
 * Create a object of FileData containing all the important file record viz. 
 * wban, yearMonthDay, time, dryBulbTemp
 */
public class FileData implements Comparable<FileData>, Serializable {

	private static final long serialVersionUID = 1L;
	private Integer wban;
	private Integer yearMonthDay;
	private Integer time;
	private Double dryBulbTemp;

    public FileData(){
    	
    }
	
	public Integer getWban() {
		return wban;
	}
	public void setWban(Integer wban) {
		this.wban = wban;
	}
	public Integer getYearMonthDay() {
		return yearMonthDay;
	}
	public void setYearMonthDay(Integer yearMonthDay) {
		this.yearMonthDay = yearMonthDay;
	}
	public Integer getTime() {
		return time;
	}
	public void setTime(Integer time) {
		this.time = time;
	}
	public Double getDryBulbTemp() {
		return dryBulbTemp;
	}
	public void setDryBulbTemp(Double dryBulbTemp) {
		this.dryBulbTemp = dryBulbTemp;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((dryBulbTemp == null) ? 0 : dryBulbTemp.hashCode());
		result = prime * result + ((time == null) ? 0 : time.hashCode());
		result = prime * result + ((wban == null) ? 0 : wban.hashCode());
		result = prime * result
				+ ((yearMonthDay == null) ? 0 : yearMonthDay.hashCode());
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
		FileData other = (FileData) obj;
		if (dryBulbTemp == null) {
			if (other.dryBulbTemp != null)
				return false;
		} else if (!dryBulbTemp.equals(other.dryBulbTemp))
			return false;
		if (time == null) {
			if (other.time != null)
				return false;
		} else if (!time.equals(other.time))
			return false;
		if (wban == null) {
			if (other.wban != null)
				return false;
		} else if (!wban.equals(other.wban))
			return false;
		if (yearMonthDay == null) {
			if (other.yearMonthDay != null)
				return false;
		} else if (!yearMonthDay.equals(other.yearMonthDay))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "FileData [wban=" + wban + ", yearMonthDay=" + yearMonthDay
				+ ", time=" + time + ", dryBulbTemp=" + dryBulbTemp + "]";
	}

	public int compareTo(FileData d) {
		Double dryBulbTemp = ((FileData)d).dryBulbTemp;
		if (this.dryBulbTemp > dryBulbTemp)
			return 1;
		else if (this.dryBulbTemp < dryBulbTemp)
			return -1;
		else
			return 0;
	}
	
}
