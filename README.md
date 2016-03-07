##### OVERVIEW

The bus location data stored in HBase table are processed separately day by day to form a csv file with arrival times for a specific day. The date of data to be processed is set as a previous day or any date from the configuration file hb.conf.server. Check Arrivals class of "busarrdistr" project for detail. The command for running the program is "HADOOP_CLATHPATH=$(hbase classpath) hadoop jar busarrival_hb.jar Main"


##### STRUCTURE OF THE INPUT DATA (HBASE TABLE)

The family 'info' of the bus data table consists of values [time, lineRef, directionRef, dateFrameRef, longitude, latitude, operatorRef, bearing, delay, vehicleRef, journeyPatternRef, originShortName, destinationShortName, originAimedDepartureTime, speed, timeAPI, timeStorage],
where
* `time` - Timestamp, a combined date and time in UTC expressed according to ISO 8601 in the format “YYYY-MM-DDThh:mm:ss.Ms+hh:mm”. It specifies the point of time when the vehicle’s activity is monitored. E.g. “2014-11-27T14:18:19.020+02:00” is 14:18:19 November 11, 2014, +02:00 Time Zone.
* `lineRef` - Integer, indicates the line number. A letter in the line number is removed  (e.g. 9K becomes 9). For the full line number check journeyPatternRef below.
* `directionRef` - Integer [1, 2], specifies the direction the bus is travelling on the line. The value can be either 1 (from origin stop to destination stop) or 2 (from destination stop back to origin stop). E.g. direction=1: from Hervanta to Keskustori, direction=2: from Keskustori to Hervanta. As a rule of thumb, direction 1 is usually the direction shown in http://aikataulut.tampere.fi/?lang=en at the left column, direction 2 in the right side column. 
* `dataFrameRef` - Date in the format “YYYY-MM-DD”, specifies the date when the vehicle started from the origin stop.
* `latitude` - Double, specifies the buses latitude coordinate in decimal degrees at the time of observation.
* `longitude` - Double, specifies the buses longitude coordinate in decimal degrees at the time of observation.
* `operatorRef` - String, specifies the name of the bus operator (e.g. TKL).
* `bearing` - Integer, specifies the azimuth angle of the bus in integer degrees. It is equal to 0 if the bus is stationary.
* `delay` - Integer, specifies the amount of seconds that the bus is delayed from its scheduled timetable. Negative if the bus is ahead of its schedule.
* `vehicleRef` - String, uniquely identifies the monitored vehicle. However, this field can be empty quite often.
* `journeyPatternRef` - String, indicates the line number. Generally, line numbers consist of only numbers, but sometimes they might contain a letter in the line name indicating small differences in the routes in comparison to the main route (e.g. 9K).
* `originShortName` - String, specifies the origin stop number where the vehicle started the journey.
* `destinationShortName` - String, specifies the destination stop number where the vehicle is heading to.
* `originAimedDepartureTime` - String, specifies the time of the departure from the origin bus stop in the format “hhmm”.
* `speed` - Double, indicates the vehicle current speed in km/h.
* `timeAPI` - Epoch unix timestamp, gives a number of seconds to time value (see the 1st variable “time”) not considering the time zone.
* `timeStorage` - Epoch unix timestamp, gives a number of seconds to the moment of receiving the data by the server. It can be used together with timeAPI to calculate the delay from data generating to data receiving.


##### CLASSES

The project contains the following classes:

* `Main` - The main class.
* `ArrivalsHbase` - A MapReduce class to process raw data for one day and form a compressed file arrivals_hb_[YYY-MM-DD].csv with arrival and departure time for each trip and each bus stop. Mapper deals with HBase records selected by scanner, Reducer is oridinary.
* `TextArrayWritable` - The TextArrayWritable type definition.

