package com.cs6240.airline;

import java.util.Collections;
import java.util.List;


/*
 * author: Shakti Patro , Kavyashree nagendrakumar
 */
public class Calculations {

	/*
	 * This method calculates mean for a airline for a month 
	 * Returns output in a map : (month airline, mean)
	 */
	public static Double calculateMean(List<Double> priceList){
		Double totalPrice = 0.0;
		for (Double price : priceList) {
			totalPrice += price;
		}
		Double avgPrice = totalPrice / priceList.size();
		return avgPrice;
	}
	
	/*
	 * This method calculates median for a airline for a month 
	 * Returns output in a map : (airline month , median)
	 */
	public static Double calculateMedian(List<Double> priceList){
		Double medianPrice = 0.0;
		Collections.sort(priceList);
		int middle = priceList.size()/2;
		if (priceList.size() % 2 == 1) {
			medianPrice = priceList.get(middle);
		} else {
			medianPrice = (priceList.get(middle - 1) + priceList.get(middle))/2;
		}
		return medianPrice;
	}
	
}
