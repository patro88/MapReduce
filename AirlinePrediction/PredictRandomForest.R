#install.packages('randomForest')
library(randomForest, quietly=TRUE)
require("plyr", quietly=TRUE)
require(ggplot2, quietly=TRUE)
require(reshape, quietly=TRUE)
args = commandArgs(trailingOnly=TRUE)
if (length(args) < 3) {
  stop("At least three arguments must be supplied (testData, trainedData)", call.=FALSE)
}
print(args)
headers <- c("year","month","carrier", "origin", "destination", "flightNumber", "flightDate", "dayOfMonth", "dayOfWeek", "hourOfDay", "crsDepTime", "crsElapsedTime", "delay", "distance", "daysTillNearestHoliday")
headersVal <- c("year","month", "joinColumn", "newDelay")

testData <- read.table(paste("https://s3-us-west-2.amazonaws.com/patrosp/output/test/", args[1], sep=""), header = FALSE, sep = "\t", col.names = headers)
testData["joinColumn"] <- paste(testData$flightNumber,"_",testData$flightDate,"_",testData$crsDepTime, sep="")

validateData <- read.table(paste("https://s3-us-west-2.amazonaws.com/patrosp/output/validate/", args[2], sep=""), header = FALSE, sep = "\t", col.names = headersVal)
validateData <- validateData[,-c(1:2)]
#print(validateData)
#testData <- table(merge(x=testData, y=validateData, by="joinColumn", all.y=TRUE))
testData <- join(x=testData, y=validateData, type = "inner")
#print(testData)
trainData <-""
for (variable in args[3:length(args)]) {
  tmpfile <- read.table(paste("https://s3-us-west-2.amazonaws.com/patrosp/output/train/", variable, sep=""), header = FALSE, sep = "\t", col.names = headers)
  trainData <- rbind(trainData, tmpfile)
}
trainData["newDelay"] <- (trainData$delay > 0)
trainData["joinColumn"] <- paste(trainData$flightNumber,"_",trainData$flightDate,"_",trainData$crsDepTime, sep="")
popular <- c("ATL", "ORD", "DFW", "LAX", "DEN", "IAH", "PHX", "SFO", "CLT", "DTW", "MSP", "LAS", "MCO", "EWR", "JFK", "LGA", "BOS", "SLC", "SEA", "BWI", "MIA", "MDW", "PHL", "SAN", "FLL", "TPA", "DCA", "IAD", "HOU","??")
carriers <- c( "9E","AA","AS","B6","DL","US","UA","VX","WN","YV", "EV" ,"F9", "FL", "HA", "MQ", "OO")
swapAirport <- function ( a )  if ( a %in% popular ) a else "??"
dropAll<-function(d) {
  d <- d[ !is.na(d$newDelay) ,]
  d <- d[ !is.na(d$crsDepTime) ,]
  d <- d[ !is.na(d$month) ,]
  d <- d[ !is.na(d$dayOfMonth) ,]
  d <- d[ !is.na(d$dayOfWeek) ,]
  d <- d[ !is.na(d$hourOfDay) ,]
  d <- d[ !is.na(d$crsElapsedTime) ,]
  d <- d[ !is.na(d$daysTillNearestHoliday) ,]
  
  d["R"]<- as.factor(d$newDelay)
  d["CSR"] <- as.integer(as.numeric(as.character(d$crsDepTime))/100)
  d["MONTH"] <- d$month
  d["DOM"] <- d$dayOfMonth
  d["DOW"] <- d$dayOfWeek
  d["HOD"] <- d$hourOfDay
  d["HOLIDAY"] <- ifelse(d$daysTillNearestHoliday >=30 , 30, d$daysTillNearestHoliday) 
  d["ELAPSE"] <- as.integer(as.numeric(as.character(d$crsElapsedTime))/10)
  d["DIST"]<- as.integer(as.numeric(d$distance)/100)
  d["ORG"] <- factor(sapply(as.character(d$origin),swapAirport),popular)
  d["DEST"] <- factor(sapply(as.character(d$destination),swapAirport),popular)
  d["UNIQUE_CARRIER"] <- factor(as.character(d$carrier),carriers)
  
  d <- d[ !is.na(d$UNIQUE_CARRIER),]
  d <- d[ !is.na(d$ELAPSE) ,]
  d <- d[ !is.na(d$HOD) ,]
  d <- d[ !is.na(d$DOW) ,]
  d <- d[ !is.na(d$DOM) ,]
  d <- d[ !is.na(d$MONTH) ,]
  d <- d[ !is.na(d$CSR) ,]
  d <- d[ !is.na(d$R) ,]
  d <- d[ !is.na(d$HOLIDAY) ,]
  d[,-c(1:17)]
}
y <- dropAll(trainData)
ytest <- dropAll(testData)
levels(ytest$UNIQUE_CARRIER) <- levels(y$UNIQUE_CARRIER)
levels(ytest$ELAPSE) <- levels(y$ELAPSE)
levels(ytest$ORG) <- levels(y$ORG) 
levels(ytest$DEST) <- levels(y$DEST) 
levels(ytest$R) <- levels(y$R) 
f <- randomForest(R ~ ., data=y, ntree=50)
p <- predict(f, ytest)

#ytest["fit"] <- paste(ytest$joinColumn, ytest[p])
data<-ytest
ac <- length(data$R)
tc <- length(data[data$R==TRUE,]$R)
fc <- length(data[data$R==FALSE,]$R)
err <- length(data[ p == TRUE & data$R == FALSE,]$R)
err2 <-length(data[ p == FALSE & data$R == TRUE,]$R)
print(paste("ac=", ac))
print(paste("tc=", tc))
print(paste("fc=", fc))
print(paste("true to false err=", err))
print(paste("false to true err=", err2))

#mytable <- table(ytest$joinColumn,p)
#print(mytable)
#write(ytest$fit, file = "predictionOutput" , append = TRUE)

varImpPlot(f, type = 2, main = "RandomForest")
x<- paste(testData[1,2], "\tac=", ac, "\ttc=", tc ,"\tfc=", fc, "\ttrue to false err=", err, "\tfalse to true err=", err2, "\tpred Ontime act Late", round(err/tc,2), "\tpred Late act Ontime",round(err2/fc,2), "\ttotal ", round((err+err2)/ac,2),"\n")
#print (x)
#write(x, "predictionResults", append = TRUE)
#w <- paste(data$joinColumn, data[p])
#print(w)
#write(w, file = "predictionOutput" , append = TRUE)