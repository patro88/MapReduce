# MapReduce
Assignments and Project for class CS6240

## A0 - Inhale.

The Bureau of Transport Statistics' On-time Performance (OTP) dataset has information about flights in the USA. The full dataset covers 27 years of air travel and is over 60GB in plain text. For this assignment you should answer the question: Which airlines have the least expensive fares? 
Fine print: 
  (0) Individual assignment. 
  (1) Data for one  month is here. 
  (2) Write a sequential Java program reading one gzipped file and writing results on the console. 
  (3) The only output should be the K and F, one per line, where K is the number of corrupt lines of input (lines not in the same format as the rest and lines with flights that do not pass the sanity test),  F is  the number of sane flights. Next, output pairs "C p" where C is a carrier two letter code  and p is the mean price of tickets. Sort the list by increasing price.  
  (4) The sanity test is: CRSArrTime and CRSDepTime should not be zero timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime; timeZone % 60 should be 0 AirportID,  AirportSeqID, CityMarketID, StateFips, Wac should be larger than 0 Origin, Destination,  CityName, State, StateName should not be empty
        For flights that not Cancelled:
          ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
          if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
          if ArrDelay < 0 then ArrDelayMinutes should be zero
          if ArrDelayMinutes >= 15 then ArrDel15 should be true
  (5) Design the code with care as you will reuse it. Document and test it.  
  (6) The reference solution is ~1KLOC and prints 4083 for K, 435940 for F and, for instance, "UA 545.62".   Processing time is ~5 seconds. 


## A1 - Threading. Airfares evolve with time, one month of traffic is not sufficient to answer which airline is the cheapest. Try with two years worth of OTP data. Improve throughput of your code with parallel processing primitives.
Fine print: (0) Individual assignment. (1) The input is "-p -input=DIR" where DIR is the path to a directory containing data files. All files in the directory will be processed. (2) The output is K and F, and a sequence of "C p m"s where m is the median ticket price. Restrict your output to airlines that are active in January 2015. (3) Clean up your code, document and test it. (4) Sample data is here. (5) Values for K and F are 128160 and 12601051. One sample airline is "AS 202.36 171.57". The reference solution is 150LOC additional/changed over A0. Processing time is under a minute.(6) Submit your assignment as a single tar.gz file which unpacks into a directory named "LastName_FirstName_A1".  That directory should contain a README file that explains how to build and run your assignment.


## A2 - Distribution. As data sizes increase the single machine version does not scale, use Map Reduce to solve A1. 
Fine print: (0) Group assignment, two students. (1) Provide code that can run in pseudo-distributed mode as well as on EMR. (3) Produce a graph that plots the average ticket price for each month for each airline.  Use R. No other output is required. (3) Include a script that executes everything and produces the graph. For example, if you use the Unix make command, you should have two targets pseudo and cloud such that typing make pseudo will create a HDFS file system, start hadoop, run your job, get the output, and produce the graph. Typing  make pseudo will run your code on EMR. (4) Only plot airlines with flights in 2015, limit yourself to the 10 airlines with the most flights overall. (5) Information on how to setup AWS is here. (6) Write a one page report that documents your implementation and that describes your results. The report should be automatically constructed as part of running the project to include the plot. (Hint: use LaTeX or Markdown) (7) Submit a tar.gz file which unpacks into a directory name "LastName1_LastName2_A2". That directory should contain a README file that explains how to build and run your code. Make sure that the code is portable. Document what it requires. (8) The reference solution builds off A1, adding 154 lines of Java code and 15 lines of R code.


## A3 - Redux. Compare the cost of computing (A) mean, (B) median, and (C) fast median for (i) singe threaded and (ii) multi-threaded Java, as well as (iii) pseudo-distributed MR and (iv) distributed MR.
Fine print: (0) Group assignment, two students. (1) Reuse previous implementations as much as possible. Output "m C v" triples where m identifies the month, C is an airline, and v is the median or mean. (2) Devise your own approach to speed up computation of the median, this may include approximation techniques. If you choose to approximate, make sure to measure accuracy.  (3) Evaluate the performance of the following configurations i-A, i-B, ii-A, ii-B, iii-A, iii-B, iii-C, iv-A, iv-B, iv-C. (4) Create a benchmarking harness that will automatically run all configurations for different input sizes and generate graphs of the performance. The harness should output timings in a CSV file and generate a  PDF using R. (5) Produce a report that describes your conclusions. Use LaTex or similar. (6) The reference solution is 350 lines of Java, 40 lines of R, and 150 line of Make.


## A4 - Regression. The price of a ticket depends, in part, on the fuel consumed, figuring out which airline is cheapest requires a little bit of work. Write a MapReduce job that ranks carriers and plots the evolution of prices of the least expensive carrier over time.
Fine Print: (0) Group assignment, two students. (1) Take as input "-time=N" where N is a natural number representing scheduled flight minutes.  (2) For each year and each carrier, estimate the intercept and the slope of a simple linear regression using the scheduled flight time to explain average ticket prices. For a given year, compare carriers at N by computing intercept+slope*N. The least expensive carrier is the carrier with lowest price at N for the most years. (3)  Plot, for each week of the entire dataset, the median price of the least expensive carrier.  (4) The reference solution is 300 LOC of Java with two MR jobs pipelined. (5) Use the full airline dataset form s3://mrclassvitek/data -- it contains 337 files! (6) Produce a report that discusses your results and includes graphs for N=1 and N=200. (7) The reference solution has 300 lines of Java.


## A5 - Paths. Compute missed connections for all two-hop paths. 
Fine Print: (0) Group assignment, two students. (1) A connection is any pair of flight F and G of the same carrier such as F.Destination = G.Origin and the scheduled departure of G is  <= 6 hours and >= 30 minutes after the scheduled arrival of F. (2) A connection is missed when the actual arrival of F < 30 minutes before the actual departure of G. (3) Optimize your code for performance. (4) Output the number of connections and missed connections per airline, per year. (5) The reference solution outputs "UA, 2013, 2826, 129" when processing the following two files. (6) The reference solution is 239 LOC Java and 10 LOC of R.  The reference solutions takes 5 minutes to run on the full data set (337 files) with a cluster of 10 large machines. (7) Your submission should include code and Makefiles. No data should be included. A report should describe your implementation and detail the timing of your results for at least the two year data set, and, if you dare, the full data set. Include results file for any runs on AWS. The report should be in PDF.


## A6 - Prediction. Build a model to predict flight delays.
Fine print: (0) Group assignment, two students. (1) Your program takes as input 36 historical files. Each file is a month of data. You can use these to build a model. (2) You program also take a single test file. This represents all the future flights we want to predict. (3) Output a file containing predictions in the format <FL_NUM>_<FL_DATE>_<CRS_DEP_TIME>, logical. The first column uniquely identifies a flight and the second is TRUE if the flight will be late. (4) Report execution time and the confusion matrix for the provided data. (5) The choice of predictive model is open; you will be graded on the accuracy of your method as well as execution time. One possible choice is to use the Random Forest algorithm. R and Java implementations exist. (6) Input data is to be found in bucked s3://mrclassvitek, in folders a6history and a6test.  The folder a6validate contains a file that has the correct answers for most flights, use it to compute the confusion matrix. (7) As a measure of accuracy, use the sum of the percentage of on-time flights misclassified as delayed and the percentage of delayed flights misclassified as on-time. (8) Some hints for using random forests: (a) split the data and build models for subsets of the entire data set. (b) Recode the data so that the type of each column has at most 50 different values. In R, they should be factors. (c) Delete columns that are not usable for predictions. (d) Synthesize features that you think make sense. For example you could create a column labelled "Holidays" and it would be true when a flight is close to Christmas, New Year, and Thanksgiving. (8) A flight is delayed is ARR_DELAY > 0.


## A7 - Routing. Given an origin, a destination and a date, propose two-hops routes that minimize the chance of missed connections. 
Fine print: (0) Group assignment, four students. (1) Your program takes 36 history files to build a model, one single test file that contains all scheduled flights for the next year, to create itineraries, and one request file in the following format  year, month, day, origin, destination, ignore. For each request, your program should propose an itinerary, written flight_num, flight_num, duration.  (3) The output of the program is scored as follows: sum the durations in hours. Add 100 hours for each missed connection. (4) Connections must have at least 30 minutes and no more than one hour on the same carrier. (5) The data is in the bucket s3://mrclassvitek, in folders a7history, a7test, a7request and a7validate.  The last directory contains a file which lists missed connections, use it for scoring. (6) The PDF report file should include details about the implementation as well as a study of the performance and accuracy of the solution. A detailed breakdown of what each team member worked should be added. Grading will include the quality of the report. All projects will be code walked.


## A8 - Spark. Reimplement A4 in Scala using Spark.
Fine print: (0) Group assignment, four students. (1) Your report should include a detailed comparison of the two solutions in terms of performance and software engineering.


## A9 - DSort. Distributed sort.
Fine print: (0) Groups of four. (1) You will build two java programs, a client and a sort node. Multiple copies of the sort node will run on Amazon EC2 instances, and the client will connect to them to issue sort tasks. Write scripts to automate creation and destruction of your cluster as well as execution and timing of sort jobs. Use this system to sort the data at s3://cs6240sp16/climate numerically on the “Dry Bulb Temp” field. (3) Your report should include execution time for 2 instances and 8 instances, as well as a list of the top 10 values in the data set (show wban #, date, time, and temperature).
Simplified sample run:
$ ./start-cluster 4
Cluster with 4 nodes started.
$ ./sort “Dry Bulb Temp” s3://cs6240sp16/climate s3://your-bucket/output
$ aws s3 ls s3://your-bucket/output
output-0000
output-0001
output-0002
output-0003
$ ./stop-cluster
4 nodes stopped
(4) Suggested Design: (a) Sample Sort: Each node reads a similar sized chunk of the input data. Since there are more files than nodes in the data, it’s sufficient to partition the input files among the nodes - no need for nodes to read partial files. Randomly sample many values from the data, and broadcast to all nodes. Each node calculates the partitioning of data between nodes, and sends its initial input records to the appropriate nodes for sorting.  Each node then sorts its partition and outputs the results to a numbered output file. As in map-reduce, concatenating the output files alphabetically should give the final output “data file” in globally sorted order. (b) Cluster management: Write your node list to a local file when creating instances. Copy this file to each instance with the node jar.  (5) Requirements:  (a) EC2 automation: Script to create a cluster of EC2 linux nodes and install your server node software. Script to destroy your EC2 cluster. (b)  Distributed Java sorting system: A node program will run on each EC2 instance. A client program will accept commands and communicate with the nodes. You must use a build tool that automatically fetches dependencies. You must generate a stand-alone JAR for the node program that can be copied to EC2 instances and executed. (c) Test case: Should be able to input and sort s3://cs6240sp16/climate, a directory of gzipped CSV files. (d) Report: Comparison of 2 and 8 node execution time. Top 10 values in data set. Discussion of design decisions and challenges. Description of which team members did what. (6) Submit source code, PDF, but no data/binaries.
