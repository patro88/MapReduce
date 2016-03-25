package com.cs6240.airlineMR;


/**
 * @author : Shakti Patro and Kavyashree Ngendrakumar 
 * @collaborator : Pankaj Tripathi
 * Main class For application.
 * Based on input , Application specific method calls are done here  
 * 
 */

import java.text.ParseException;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Days;

public class Utils{
	public static int closerDate(Date originalDate, List<Date> dateList) {
	    Collections.sort(dateList);
	    Iterator<Date> iterator = dateList.iterator();
	    Date previousDate = null;
	    while (iterator.hasNext()) {
	        Date nextDate = iterator.next();
	        if (nextDate.before(originalDate)) {
	            previousDate = nextDate;
	            continue;
	        } else if (nextDate.after(originalDate)) {
	            if (previousDate == null || isCloserToNextDate(originalDate, previousDate, nextDate)) {
	            	return Math.abs(Days.daysBetween(new DateTime(originalDate),new DateTime(nextDate)).getDays());
	            }
	        } else {
	        	return Math.abs(Days.daysBetween(new DateTime(originalDate),new DateTime(nextDate)).getDays());
	        }
	    }
	    return Math.abs(Days.daysBetween(new DateTime(originalDate), new DateTime(previousDate)).getDays());
	}

	private static boolean isCloserToNextDate(Date originalDate, Date previousDate, Date nextDate) {
	    if(previousDate.after(nextDate))
	        throw new IllegalArgumentException("previousDate > nextDate");
	    return ((nextDate.getTime() - previousDate.getTime()) / 2 + previousDate.getTime() <= originalDate.getTime());
	}
	
	public static List<Date> getHolidays(Date fldate) throws ParseException{
		List<Date> holidays=new LinkedList<>();
		Calendar cal=Calendar.getInstance();
		cal.setTime(fldate);

        // check if New Year's Day
        cal.set(Calendar.MONTH, 11);
        cal.set(Calendar.DATE, 31);
		holidays.add(cal.getTime());
		// check if Christmas
		cal.set(Calendar.MONTH, 11);
        cal.set(Calendar.DATE, 25);
		holidays.add(cal.getTime());
		// check if 4th of July
		cal.set(Calendar.MONTH, 6);
        cal.set(Calendar.DATE, 4);
		holidays.add(cal.getTime());
		// check Thanksgiving (4th Thursday of November)
		cal.set(Calendar.MONTH, 10);
		cal.set(Calendar.DAY_OF_WEEK_IN_MONTH,4);
        cal.set(Calendar.DAY_OF_WEEK, Calendar.THURSDAY);
		holidays.add(cal.getTime());
		// check Labor Day (1st Monday of September)
		cal.set(Calendar.MONTH, 8);
		cal.set(Calendar.DAY_OF_WEEK_IN_MONTH,1);
		cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		holidays.add(cal.getTime());
		// check President's Day (3rd Monday of February)
		cal.set(Calendar.MONTH, 1);
		cal.set(Calendar.DAY_OF_WEEK_IN_MONTH,3);
		cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		holidays.add(cal.getTime());
		// check Veterans Day (November 11)
		cal.set(Calendar.MONTH, 10);
        cal.set(Calendar.DATE, 11);
		holidays.add(cal.getTime());
		// check MLK Day (3rd Monday of January)
		cal.set(Calendar.MONTH, 0);
		cal.set(Calendar.DAY_OF_WEEK_IN_MONTH,3);
		cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		holidays.add(cal.getTime());
	
		return holidays;	
	}
}


