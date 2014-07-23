Optimised-K_Means
=================

This project is related to the optimisation of the traditional K-Means algoritm in the parallel enviroment(Hadoop). Optimisation 
in this context means that we are trying to use better initiation techniques other than random initiation or the one used in K-Means++.
The Java code contains the required MapReduce classes to run the Job.









Steps to run the code:


1]For compilation :Enter in the directory where code.java is stored ..create a new folder, name it "classes" and use the command below

--->javac -classpath `hadoop classpath` -d classes ./SeedOptimised_KMeans.java



2]Converting it in .jar from .java:

--->jar -cvf SeedOptimised_KMeans.jar -C classes/ .

3]Running the jar file....or the M/R code...

--->hadoop jar SeedOptimised_KMeans.jar org.myorg.SeedOptimised_KMeans  /data/input /data/output 4 3 5250 /data/centroidOutput

[The arguments used here for running the M/R Code are: InputAddress--OutputAddress--No of Dimensions--No of Clusters--No of KEY/Value Pairs CentroidOutputAddress.]


****************************
1] Dont leave more than one blank row at the end of dataset...else you will get null pointer exception..
