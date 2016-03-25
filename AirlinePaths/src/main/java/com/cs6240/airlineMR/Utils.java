package com.cs6240.airlineMR;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Utils {

	static SimpleDateFormat format1 = new SimpleDateFormat("MM/dd/yyyy HH:mm");
	static SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm");

	public static boolean isFirstDayAndInRange(long timeInMillis) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timeInMillis);
		return ((calendar.getMinimum(Calendar.DATE) == calendar.get(Calendar.DATE)) 
				&& (calendar.get(Calendar.HOUR_OF_DAY) <= 6));
	}

	public static boolean isLastDayAndInRange(long timeInMillis) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timeInMillis);
		return ((calendar.getMaximum(Calendar.DAY_OF_MONTH) == calendar.get(Calendar.DATE))
				&& calendar.get(Calendar.HOUR_OF_DAY) >= 18);
	}


	/**
	 * Get the millisecond value from a date string in hhmm format
	 * the flDate is used to get the date part and hhmm is used for hour and minute part
	 * flDate can come in two formats as per the input files , so two formatters are used
	 * hhmm can be 1, 2, 3 or 4 characters long. So used if conditions to append 0 at the begenning.
	 * 
	 * @throws ParseException 
	 * 
	 */

	public static long getTimeFromString(String flightDate, String hhmmString) throws ParseException {

		if(hhmmString.length() == 3){
			hhmmString = "0"+hhmmString;
		}else if(hhmmString.length() == 2) {
			hhmmString = hhmmString + "00";
		}else if(hhmmString.length() == 1) {
			hhmmString = hhmmString + "000";
		}
		String fomrattedDate = flightDate + " " + hhmmString.substring(0,2) + ":"  + hhmmString.substring(2);
		Date date;
		if(flightDate.contains("-")) { 
			date = format2.parse(fomrattedDate);
		}
		else { 
			date = format1.parse(fomrattedDate);
		}
		return date.getTime();

	}
}

