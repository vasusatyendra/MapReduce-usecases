# Wikipedia_GeoLocation 
Map Reduce Program to group articles in Wikipedia by their GEO location.

Input File: http://downloads.dbpedia.org/3.2/bg/  ---> geo_bg.csv.bz2 

Introduction:
Db-Pedia has a nice data dump of geo locations of wikipedia articles the objective is to group articles to-gather by their geo location and dump the output.

Data Set: 
Geo Location Data From DB-Pedia in CSV format Geographic coordinates extracted from Wikipedia.

Details: Mapper
The Mapper here is responsible for just extracting the right geo location from the given set (please refer data) and round the latitude and the longitude

Details: Reducer
The reducer just groups the data and writes the output with comma separation
