Authors: Anthony Rezzonico, Bruce Darcy, Daniel Rhoades

This is our project looking at AirBnb data and trying to learn socio-economic factors of regions.
Given the AirBnb data set, we analyzed the common amenities for regions and ranked them by our approximation for the region's standard of living.

We derived a cost of living like score from the price per person/night of our dataset, then compared it for countries, and different housing types.

We looked at amenities that hosts describe in their listings, such as a TV or washing machine. We used that data to assign a value to that ammenity based on how expensive listings with it tended to be.

That let us compare the value of different amenities within a region, and the value of the same amenity in another country, or if it were in a different housing type. For example we can compare the value of a washing machine in an appartment in Canada compared to a house in Canada.

We also created a set of essential amenities for each country and housing type. These are amenities that more than 40% of listings contained, indicating that they seem to be important for someone to have in that region. 

Outputs from all the things mentioned above are in the resources directory, and named accordingly to what they represent.

Source code and Spark jobs are contained within the AirYourBnb directory.
* To run the spark programs use the run-job.sh script:
* * Usage: ./run-sob.sh[namenode] [portnumber] [class of driver] [jar name in target dir] [args]
* * The args parameter is a type that define which HDFS cluster to use
* * possible jobs are: AirbnbAnalysis, JustCountry, and Numbeo

Outputs from the program and other relevant documents are contained within the resources directory.

