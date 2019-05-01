Authors: Anthony Rezzonico, Bruce Darcy, Daniel Rhoades

This is our project looking at AirBnb data and trying to learn socio-economic factors of regions.
Given the AirBnb data set, we analyzed the common amenities for regions and ranked them by our approximation for the region's standard of living.

Source code and Spark jobs are contained within the AirYourBnb directory.
* To run the spark programs use the run-job.sh script:
* * Usage: ./run-sob.sh[namenode] [portnumber] [class of driver] [jar name in target dir] [args]
* * The args parameter is a type that define which HDFS cluster to use
* * possible jobs are: AirbnbAnalysis, JustCountry, and Numbeo

Outputs from the program and other relevant documents are contained within the resources directory.

