package com.cs6240.airlineMR;

import org.apache.commons.lang.StringUtils;

import com.cs6240.exception.InvalidFormatException;


/*
 * @author: Shakti Patro , Kavyashree nagendrakumar
 *
 * Class : Encapsulates all airlines realted data
 */
public class AirlineDetailsPojo {

	private Integer year;
	private Integer month;
	private String dayOfWeek;
	private String dayOfMonth;
	public String getDayOfWeek() {
		return dayOfWeek;
	}
	public void setDayOfWeek(String dayOfWeek) {
		this.dayOfWeek = dayOfWeek;
	}
	public String getDayOfMonth() {
		return dayOfMonth;
	}
	public void setDayOfMonth(String dayOfMonth) {
		this.dayOfMonth = dayOfMonth;
	}

	private String carrier;
	private String origin;
	private String destination;
	private String fLDate;
	private String flightNumber;
	private Integer originAirportId;
	private Integer originAirportSequenceId;
	private Integer originCityMarketId;
	private Integer originStateFips;
	private Integer originWac;
	private String originCityName;
	private String originStateAbbr;
	private String originStateName;
	private Integer destinationAirportId;
	private Integer destinationAirportSequenceId;
	private Integer destinationCityMarketId;
	private Integer destinationStateFips;
	private Integer destinationWac;
	private String destinationCityName;
	private String destinationStateAbbr;
	private String destinationStateName;
	private int crsArrivalTime;
	private int crsDepartureTime;
	private int crsElapsedTime;
	private int actualArrivalTime;
	private int actualDepartureTime;
	private int actualElapsedTime;
	private int arrivalDelay;
	private int arrivalDelayMinutes;
	private int arrivalDelay15;
	private String cancelled;
	private double price;
	private String distance;
	
	
	public String getFlightNumber() {
		return flightNumber;
	}
	public void setFlightNumber(String flightNumber) {
		this.flightNumber = flightNumber;
	}
	public String getDistance() {
		return distance;
	}
	public void setDistance(String distance) {
		this.distance = distance;
	}
	public Integer getYear() {
		return year;
	}
	public void setYear(Integer year) {
		this.year = year;
	}
	public Integer getMonth() {
		return month;
	}
	public void setMonth(Integer month) {
		this.month = month;
	}
	public String getfLDate() {
		return fLDate;
	}
	public void setfLDate(String fLDate) {
		this.fLDate = fLDate;
	}
	public String getCarrier() {
		return carrier;
	}
	public void setCarrier(String carrier) {
		this.carrier = carrier;
	}
	public String getOrigin() {
		return origin;
	}
	public void setOrigin(String origin) {
		this.origin = origin;
	}
	public String getDestination() {
		return destination;
	}
	public void setDestination(String destination) {
		this.destination = destination;
	}
	public String getDestinationCityName() {
		return destinationCityName;
	}
	public void setDestinationCityName(String destinationCityName) {
		this.destinationCityName = destinationCityName;
	}
	public Integer getOriginAirportId() {
		return originAirportId;
	}
	public void setOriginAirportId(Integer originAirportId) {
		this.originAirportId = originAirportId;
	}
	public Integer getOriginAirportSequenceId() {
		return originAirportSequenceId;
	}
	public void setOriginAirportSequenceId(Integer originAirportSequenceId) {
		this.originAirportSequenceId = originAirportSequenceId;
	}
	public Integer getOriginCityMarketId() {
		return originCityMarketId;
	}
	public void setOriginCityMarketId(Integer originCityMarketId) {
		this.originCityMarketId = originCityMarketId;
	}
	public Integer getOriginStateFips() {
		return originStateFips;
	}
	public void setOriginStateFips(Integer originStateFips) {
		this.originStateFips = originStateFips;
	}
	public Integer getOriginWac() {
		return originWac;
	}
	public void setOriginWac(Integer originWac) {
		this.originWac = originWac;
	}
	public String getOriginCityName() {
		return originCityName;
	}
	public void setOriginCityName(String originCityName) {
		this.originCityName = originCityName;
	}
	public String getOriginStateAbbr() {
		return originStateAbbr;
	}
	public void setOriginStateAbbr(String originStateAbbr) {
		this.originStateAbbr = originStateAbbr;
	}
	public String getOriginStateName() {
		return originStateName;
	}
	public void setOriginStateName(String originStateName) {
		this.originStateName = originStateName;
	}
	public Integer getDestinationAirportId() {
		return destinationAirportId;
	}
	public void setDestinationAirportId(Integer destinationAirportId) {
		this.destinationAirportId = destinationAirportId;
	}
	public Integer getDestinationAirportSequenceId() {
		return destinationAirportSequenceId;
	}
	public void setDestinationAirportSequenceId(Integer destinationAirportSequenceId) {
		this.destinationAirportSequenceId = destinationAirportSequenceId;
	}
	public Integer getDestinationCityMarketId() {
		return destinationCityMarketId;
	}
	public void setDestinationCityMarketId(Integer destinationCityMarketId) {
		this.destinationCityMarketId = destinationCityMarketId;
	}
	public Integer getDestinationStateFips() {
		return destinationStateFips;
	}
	public void setDestinationStateFips(Integer destinationStateFips) {
		this.destinationStateFips = destinationStateFips;
	}
	public Integer getDestinationWac() {
		return destinationWac;
	}
	public void setDestinationWac(Integer destinationWac) {
		this.destinationWac = destinationWac;
	}
	public String getDestinationName() {
		return destinationCityName;
	}
	public void setDestinationName(String destinationName) {
		this.destinationCityName = destinationName;
	}
	public String getDestinationStateAbbr() {
		return destinationStateAbbr;
	}
	public void setDestinationStateAbbr(String destinationStateAbbr) {
		this.destinationStateAbbr = destinationStateAbbr;
	}
	public String getDestinationStateName() {
		return destinationStateName;
	}
	public void setDestinationStateName(String destinationStateName) {
		this.destinationStateName = destinationStateName;
	}
	public int getCrsArrivalTime() {
		return crsArrivalTime;
	}
	public void setCrsArrivalTime(int crsArrivalTime) {
		this.crsArrivalTime = crsArrivalTime;
	}
	public int getCrsDepartureTime() {
		return crsDepartureTime;
	}
	public void setCrsDepartureTime(int crsDepartureTime) {
		this.crsDepartureTime = crsDepartureTime;
	}
	public int getCrsElapsedTime() {
		return crsElapsedTime;
	}
	public void setCrsElapsedTime(int crsElapsedTime) {
		this.crsElapsedTime = crsElapsedTime;
	}
	public int getActualArrivalTime() {
		return actualArrivalTime;
	}
	public void setActualArrivalTime(int actualArrivalTime) {
		this.actualArrivalTime = actualArrivalTime;
	}
	public int getActualDepartureTime() {
		return actualDepartureTime;
	}
	public void setActualDepartureTime(int actualDepartureTime) {
		this.actualDepartureTime = actualDepartureTime;
	}
	public int getActualElapsedTime() {
		return actualElapsedTime;
	}
	public void setActualElapsedTime(int actualElapsedTime) {
		this.actualElapsedTime = actualElapsedTime;
	}
	public int getArrivalDelay() {
		return arrivalDelay;
	}
	public void setArrivalDelay(int arrivalDelay) {
		this.arrivalDelay = arrivalDelay;
	}
	public int getArrivalDelayMinutes() {
		return arrivalDelayMinutes;
	}
	public void setArrivalDelayMinutes(int arrivalDelayMinutes) {
		this.arrivalDelayMinutes = arrivalDelayMinutes;
	}
	public int getArrivalDelay15() {
		return arrivalDelay15;
	}
	public void setArrivalDelay15(int arrivalDelay15) {
		this.arrivalDelay15 = arrivalDelay15;
	}
	public String getCancelled() {
		return cancelled;
	}
	public void setCancelled(String cancelled) {
		this.cancelled = cancelled;
	}
	
	public double getPrice() {
		return price;
	}
	public void setPrice(double price) {
		this.price = price;
	}
	
	/*
	 * Constructor : Takes an array of flight details with size 110.
	 * This constructor sets in values by the column numbers 
	 * Note: Size is assumed to be validated before constructor call.
	 */
	public AirlineDetailsPojo(String[] flightDetails) throws InvalidFormatException{
		try{
			this.year = Integer.parseInt(flightDetails[0]);
			this.month = Integer.parseInt(flightDetails[2]);
			this.dayOfMonth = flightDetails[3];
			this.dayOfWeek = flightDetails[4];
			this.fLDate = flightDetails[5];
			this.carrier = flightDetails[8]; 
			this.flightNumber = flightDetails[10];
			this.originAirportId = Integer.parseInt(flightDetails[11]);
			this.originAirportSequenceId = Integer.parseInt(flightDetails[12]);
			this.originCityMarketId = Integer.parseInt(flightDetails[13]);
			this.origin = flightDetails[14];
			this.originCityName = flightDetails[15];
			this.originStateAbbr = flightDetails[16];
			this.originStateFips = Integer.parseInt(flightDetails[17]);
			this.originStateName = flightDetails[18];
			this.originWac = Integer.parseInt(flightDetails[19]);
			this.destinationAirportId = Integer.parseInt(flightDetails[20]);
			this.destinationAirportSequenceId = Integer.parseInt(flightDetails[21]);
			this.destinationCityMarketId = Integer.parseInt(flightDetails[22]);
			this.destination = flightDetails[23];
			this.destinationCityName = flightDetails[24];
			this.destinationStateAbbr = flightDetails[25];
			this.destinationStateFips = Integer.parseInt(flightDetails[26]);
			this.destinationStateName = flightDetails[27];
			this.destinationWac = Integer.parseInt(flightDetails[28]);
			
			if(StringUtils.isNotEmpty(flightDetails[41]) 
					&& StringUtils.isNotEmpty(flightDetails[30]) 
					&& StringUtils.isNotEmpty(flightDetails[51])) {
				this.crsDepartureTime = Integer.parseInt(flightDetails[29]);
				this.actualDepartureTime = Integer.parseInt(flightDetails[30]);
				this.crsArrivalTime = Integer.parseInt(flightDetails[40]);
				this.actualArrivalTime = Integer.parseInt(flightDetails[41]);
				this.crsElapsedTime = Integer.parseInt(flightDetails[50]);
				this.actualElapsedTime = Integer.parseInt(flightDetails[51]);
			}
			if(StringUtils.isNotEmpty(flightDetails[42])) {
				this.arrivalDelay = (int)Double.parseDouble(flightDetails[42]);
				this.arrivalDelayMinutes = (int)Double.parseDouble(flightDetails[43]);
				this.arrivalDelay15 = (int)Double.parseDouble(flightDetails[44]);
			}
			this.cancelled = flightDetails[47];
			if(StringUtils.isNotEmpty(flightDetails[109]))
				this.price = Double.parseDouble(flightDetails[109]);
			this.distance = flightDetails[54];
		} catch (NumberFormatException e) {
			throw new InvalidFormatException("String in place of a number.");
		} catch (NullPointerException e) {
			throw new InvalidFormatException("Null got where not expected.");
		}
	}	
	
	
	public AirlineDetailsPojo(String[] flightDetails, boolean isTestData) throws InvalidFormatException{
		try{
			this.year = Integer.parseInt(flightDetails[0]);
			this.month = Integer.parseInt(flightDetails[2]);
			this.dayOfMonth = flightDetails[3];
			this.dayOfWeek = flightDetails[4];
			this.flightNumber = flightDetails[10];
			this.carrier = flightDetails[8];
			this.fLDate = flightDetails[5];
			this.origin = flightDetails[14];
			this.destination = flightDetails[23];
			if(StringUtils.isNotEmpty(flightDetails[41]) 
					&& StringUtils.isNotEmpty(flightDetails[30]) 
					&& StringUtils.isNotEmpty(flightDetails[51])
					&& !flightDetails[41].equals("NA") 
					&& !flightDetails[30].equals("NA") 
					&& !flightDetails[51].equals("NA")) {
				this.actualArrivalTime = Integer.parseInt(flightDetails[41]);
				this.actualDepartureTime = Integer.parseInt(flightDetails[30]);
				this.actualElapsedTime = Integer.parseInt(flightDetails[51]);
			}
			this.crsDepartureTime = Integer.parseInt(flightDetails[29]);
			this.crsElapsedTime = Integer.parseInt(flightDetails[50]);
			this.crsArrivalTime = Integer.parseInt(flightDetails[40]);
			
			if(StringUtils.isNotEmpty(flightDetails[42]) && !flightDetails[42].equals("NA")) {
				this.arrivalDelay = (int)Double.parseDouble(flightDetails[42]);
				this.arrivalDelayMinutes = (int)Double.parseDouble(flightDetails[43]);
				this.arrivalDelay15 = (int)Double.parseDouble(flightDetails[44]);
			}
			this.cancelled = flightDetails[47];
			if(StringUtils.isNotEmpty(flightDetails[109]) && !flightDetails[109].equals("NA"))
				this.price = Double.parseDouble(flightDetails[109]);
			this.distance = flightDetails[54];

		} catch (NumberFormatException e) {
			throw new InvalidFormatException("String in place of a number.");
		} catch (NullPointerException e) {
			throw new InvalidFormatException("Null got where not expected.");
		}
	}	
	
}
