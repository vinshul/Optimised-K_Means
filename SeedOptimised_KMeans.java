package org.myorg;
import java.lang.*;
import java.io.IOException;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Reducer;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.collections.MultiMap;

@SuppressWarnings("deprecation")
public class SeedOptimised_KMeans 
{
	public static String OUT = "OutputFile";
	public static String IN = "InputFile";
	public static String OUTPUT_FILE_NAME = "/part-00000";
	public static String DATA_FILE_NAME = "/Data.txt";
	public static String JOB_NAME = "SeedOptimised_KMeans";
	public static String SPLITTER = "::";
	public static MultiMap arrayOfCentroids = new MultiValueMap();
	public static String Dimensions ="dimensions_of_data";
	public static String No_Of_Clusters = "No_of_clusters_required";
	public static String No_Of_Keys = "no_of_input_keys";
	public static int iteration = 0;
	
	
	
	/****************************************************************************************************************************************************************
	 * This Map funtion will map the key/value pairs in a different space so as to get more closer to the density of the data points for better seed initialisation.*
	 * **************************************************************************************************************************************************************/
	
	
	public static class CentroidMapper extends MapReduceBase implements Mapper<LongWritable,Text,DoubleWritable,Text> 
	{	
		private int dimensions ;
		private int no_of_clusters;
		private int no_of_keys;						
		@Override
		public void configure(JobConf conf2) 
				{
					dimensions = conf2.getInt(SeedOptimised_KMeans.Dimensions,2);
					no_of_clusters = conf2.getInt(SeedOptimised_KMeans.No_Of_Clusters,2);
					no_of_keys= conf2.getInt(SeedOptimised_KMeans.No_Of_Keys,100);
				}
		
		@Override
		public void map (LongWritable key,Text value,OutputCollector<DoubleWritable,Text> output,Reporter reporter)throws IOException
        {          											
			String temp = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(temp);//breaking the line into tokens		
			List<Double> Ini_clusterData = new ArrayList<Double>();			
			while (tokenizer.hasMoreTokens())
				{
					Ini_clusterData.add(Double.parseDouble(tokenizer.nextToken()));
					
				}		
			//now till here we have just obtained the input value into the arraylist Ini_clusterdata...
			
			int t =0;
			double sum=0;
			int powerIndex=0;
			while(t+1<dimensions)
			{
				//this is the function used to map values...f(a,b)=(a+b)*a/b					
				sum=(Ini_clusterData.get(t)+Ini_clusterData.get(t+1))*(Ini_clusterData.get(t+1)/Ini_clusterData.get(t)) + sum ;
				t++;	
			}

			int s = 0;
			String Ini_cluster_point = "" ;
							
			while(s<dimensions)
			{				
				Ini_cluster_point =  Ini_cluster_point + Double.toString(Ini_clusterData.get(s)) 	;	
				
				//to add " " after every dimension 								
				if(s< dimensions-1)
					{
						Ini_cluster_point = Ini_cluster_point + " " 	;	
					}
				s++;			
			}				
						
			output.collect(new DoubleWritable(sum),new Text(Ini_cluster_point));
        }
	}
	
	/***************************************************************************************************
	 * This Reduce funtion will finally calculate the k-centroid seeds for the initialisation process. *
	 * *************************************************************************************************/
	
	public static class CentroidReducer extends MapReduceBase implements Reducer<DoubleWritable, Text, Text, Text> 
			{
				
				private int dimensions ;
				private int no_of_clusters;
				private int no_of_keys;
				private int emit_count=0;
				private int keys_in_cluster=0;
				private int loopindex = 0;
				private int temp=0;
				private int avgIndex=0;
				//requiredAvgIndex::its the no. of keys we are using to get the approx (average value) of just one cluster centroid...so 7 means we are using 7 key/value pairs to approximate one centroid. 
				private int requiredAvgIndex=7;
				
				List<Double> tempAvgData = new ArrayList<Double>();
				//this list will be used to store the avg of the values ...to finally evaluate the centroids!!
				
				
				
				
				@Override
				public void configure(JobConf conf2) 
						{
							dimensions = conf2.getInt(SeedOptimised_KMeans.Dimensions,2);
							no_of_clusters = conf2.getInt(SeedOptimised_KMeans.No_Of_Clusters,2);
							no_of_keys= conf2.getInt(SeedOptimised_KMeans.No_Of_Keys,100);
							
							keys_in_cluster = no_of_keys/no_of_clusters;
							
							//Initialising list to stay away from null pointer exception...
							int r=0;
							while (r<dimensions)
								{
									tempAvgData.add(0.0);
									r++;
								}	
						}
								
				@Override
				public void reduce(DoubleWritable key, Iterator<Text> values,OutputCollector<Text, Text> output, Reporter reporter) throws IOException
					{						
					  //it will remain true till avgIndex==requiredAvgIndex, as after that we have  done ...temp = temp + keys_in_cluster
					  // so it will become true again after the loopindex becomes equal or greater than temp
					  if(loopindex>=temp && emit_count<no_of_clusters)
					    {	
							//this if() function will be true only--> when we select a group to obtain the centroid, so it will be true to a exact no. of times equal to the no_of_clusters..							
							if(loopindex == temp) 
							{	int g=0;
								while (g<dimensions)
									{
										tempAvgData.set(g,0.0);
										g++;
									}				
							}
													
							if(avgIndex<requiredAvgIndex)
								{
									if(values.hasNext())
									{
										StringTokenizer tokenizer = new StringTokenizer(values.next().toString());
										int s=0;
										while (tokenizer.hasMoreTokens() && s<dimensions)
											{
												tempAvgData.set(s,tempAvgData.get(s)+Double.parseDouble(tokenizer.nextToken()));		
												//Now what above method does is it just adds up all the dimensions of the current value to dimensions of the previous value..
												s++;							
											}
										avgIndex++;									
									}								
								}
														
							if(avgIndex==requiredAvgIndex)
								{										
									int s=0;
									while (s<dimensions)
										{
											tempAvgData.set(s,tempAvgData.get(s)/requiredAvgIndex);		
											//Now we finally have the required averaged centroid....in array list tempAvgData!!
											s++;							
										}	
									
									int l=0;																				
									String Final_cluster_centroid = "" ;													
									while(l<dimensions)
									{				
										Final_cluster_centroid =  Final_cluster_centroid + Double.toString(tempAvgData.get(l)) 	;											
										//to add " " after every dimension 								
										if(l< dimensions-1)
											{
												Final_cluster_centroid = Final_cluster_centroid + " " 	;	
											}
										l++;			
									}								
								    
								    output.collect(new Text(Final_cluster_centroid),new Text("::"));
								    temp = temp + keys_in_cluster;			
								    emit_count++;		
								    avgIndex=0;//so we can now restart the averaging process to find a new centroid..					
								    
								  
										
								}		
						}						
					  loopindex++;
					
					}
			}
	
	
	
	/*************************************************************************************************************************************
	 *This is the main Map function. ALll it does is...it assigns the input data point to its nearest cluster according to the distance. *
	 *************************************************************************************************************************************/
	public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text> 
	{			 
			private int dimensions ;
			private int no_of_clusters;
						
			//In this configure function we are reading the file (i.e. the centroids calculated above) Distributed Cache and then storing the centroids evaluated into arrayOfCentroids multimap.			
			@Override
			public void configure(JobConf conf) 
			{	
				dimensions = conf.getInt(SeedOptimised_KMeans.Dimensions,2);
				no_of_clusters = conf.getInt(SeedOptimised_KMeans.No_Of_Clusters,2);			
				try 
				{
					// Fetching the file from the Distributed Cache & store the k-centroids in the arrayOfCentroids multimap.
					Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
					if (cacheFiles != null && cacheFiles.length > 0)
						{
							String line;							
							BufferedReader cacheReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
							try 
								{
									int key=0;
									while ((line = cacheReader.readLine()) != null) 
										{
											String[] temp = line.split("::");//splitting the value input by using :: as a splitter
											StringTokenizer tokenizer = new StringTokenizer(temp[0]);//breaking the line into tokens				
											while (tokenizer.hasMoreTokens())
												{
													arrayOfCentroids.put(key,Double.parseDouble(tokenizer.nextToken()));
													
												}											
											key++;											
											//now the centroids are in the array--> arrayOfCentroids and its data is in centroiddata Array										
										}
								}   								
							finally 
									{
										cacheReader.close();
									}
						}
				} 
				
				catch (IOException e)
				 {
					System.err.println("Exception reading DistribtuedCache: " + e);
				 }
			}	
						
			@Override
			public void map (LongWritable key, Text value,OutputCollector<Text,Text> output,Reporter reporter) throws IOException 
				{																										
							String temp = value.toString();
							StringTokenizer tokenizer = new StringTokenizer(temp);//breaking the line into tokens						
							List<Double> clusterData = new ArrayList<Double>();							
							while (tokenizer.hasMoreTokens())
								{
									clusterData.add(Double.parseDouble(tokenizer.nextToken()));									
								}
							
							//Till here we have just obtained the input value into the arraylist clusterdata...
							//So now we will iterate over each cluster centroid to check in which cluster this value lies													
							double min = Double.MAX_VALUE ;							
							List<Double> nearestCluster = new ArrayList<Double>();							
							int i=0;						
							while(i<no_of_clusters)
								{																		
									List list = (List) arrayOfCentroids.get(i);									
									List<Double> tempCentroidData = new ArrayList<Double>(list);//this created a copy of the ith centoidData stored in arrayOfCentroids multimap.. we will use this array list for calculations..									
									List<Double> actualCentroidData = new ArrayList<Double>(list);//this also created a copy of the ith centoidData stored in arrayOfCentroids multimap									
									double distance = 0;									
									int j=0;
									while (j<dimensions)
										{
											tempCentroidData.set(j,Math.abs(tempCentroidData.get(j)-clusterData.get(j))) ;
											//calculating the difference so as to finally calculate the distance from the array tempCentroidData
											j++;
										}
									
									int t =0;
									while(t<dimensions)
										{
											distance = tempCentroidData.get(t)*tempCentroidData.get(t) + distance;
											t++;																								
										}
									
									if (distance < min)
											{												                   
												nearestCluster = actualCentroidData;
												min = distance; 				
											} 									
									i++;
									//after the k iterations we will have the nearestCluster array containing the cluster that point belongs to..									
								}																															
														
							//converting the data into string finally so as to transfer effectively from map stage..
							String cluster_centroid = "" ;
							String cluster_point = "" ;							
							int s = 0;
							while(s<dimensions)
							{	
								cluster_centroid = cluster_centroid + " " + Double.toString(nearestCluster.get(s)) ;
								
								cluster_point =  cluster_point + Double.toString(clusterData.get(s)) 	;	
								
								//to add a "," after every dimension 								
								if(s< dimensions-1)
									{
										cluster_point = cluster_point + "," 	;	
									}
								s++;			
							}				
														
							output.collect(new Text(cluster_centroid),new Text(cluster_point));							
							// so the output of the map function will be like.....-> 123 234::	129,190 129,150....as map introduces a tab in between key and value pairs...so we can effectively distuinguish between key & its values
				
				}
	 }

	
	
	       
	/**********************************************************************************************************
	 * Reduce function will recalculate the next center for these points and emit it for reclustering process.*
	 **********************************************************************************************************/
	
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> 
			{			
				private int dimensions ;
				private int no_of_clusters;
				
				@Override
				public void configure(JobConf conf) 
						{
							dimensions = conf.getInt(SeedOptimised_KMeans.Dimensions,2);
							no_of_clusters = conf.getInt(SeedOptimised_KMeans.No_Of_Clusters,2);
						}				
				
				@Override
				public void reduce(Text key, Iterator<Text> values,
						OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException
						 {																																												
								List<Double> new_centroid = new ArrayList<Double>();
								
								//This is done to initialise the array list to stay away from null pointer exceptions,etc
								int e=0;
								while (e<dimensions)
									{
										new_centroid.add(e,0.0);
										e++;
									}
								
								double totalValues = 0 ; 
								String cluster_values = "" ;
								while(values.hasNext())
											{													
												List<Double> new_value = new ArrayList<Double>();												
												int f=0;
												while (f<dimensions)
													{
														new_value.add(f,0.0);
														f++;
													}
																																															
												String  tempnew = values.next().toString();																								
												String[] temp1 = tempnew.split(",");//to seperate different dimensions of a data point
												
												cluster_values = cluster_values + "[" ;
												int j=0;												
												while (j<dimensions)											
													{
														cluster_values = cluster_values + temp1[j] ;
														
														new_value.set(j,Double.parseDouble(temp1[j])) ;
														
														if(j< dimensions-1)
															{
																 cluster_values = cluster_values + "|" 	;	
															}																																																																																	 
														j++;
													}
												//till now we have just converted the values from string to a double....
																																															
												int p = 0;
												while (p<dimensions)
													{
														new_centroid.set(p, new_value.get(p) + new_centroid.get(p)) ;
														p++ ;
													}
												//upper while loop will keep on adding all the values till p<dimensions is true.. to finally get the sum
												
												totalValues++ ;												
												cluster_values = cluster_values + "]--" ;
											}
								
								//finally now we have totalvalues and the sum of all the data points ...to calculate the centroid														
								int ite = 0;
								while(ite<dimensions)
									{
										new_centroid.set(ite, new_centroid.get(ite)/totalValues) ;
										ite++ ;
									}
													
					            //converting the centroiddata into string for output..
								String newcluster_centroid = "" ;
								
								int n = 0 ;
								while(n<dimensions)
								{	
									newcluster_centroid = newcluster_centroid + " " + Double.toString(new_centroid.get(n)) ;		
										
									if(n==dimensions-1)
										{ 
											newcluster_centroid = newcluster_centroid + "::" ;
										} 									
									n++;	
								}																		
								output.collect(new Text(newcluster_centroid),new Text(cluster_values));
								//Finally the reducer will output the new cluster centroids that will be used in the next iteration and the clustered data points from which these centroids are calculated..
						}
			}

	
	
	
		/****************************************************
		 * Run Function --> Driver Function for this M/R Job*
		 ****************************************************/
	 
		public static void main(String[] args) throws Exception
		 {
			run(args);
		 }

		public static void run(String[] args) throws Exception
		{
			IN = args[0];
			OUT = args[1];
			int d = Integer.parseInt(args[2]);
			int k = Integer.parseInt(args[3]);
			int nkeys = Integer.parseInt(args[4]);
			String input = IN;
			String output = OUT + System.nanoTime();
			String centroidOutput = args[5];
			
			//Job for the centroid initialisation...
			 
			 JobConf conf2 = new JobConf(SeedOptimised_KMeans.class);
	 	     conf2.setJobName("CentroidInitialisation");
			 conf2.setInt(Dimensions,d);
			 conf2.setInt(No_Of_Clusters,k);
			 conf2.setInt(No_Of_Keys,nkeys);
	 	     conf2.setMapOutputKeyClass(DoubleWritable.class);
             conf2.setMapOutputValueClass(Text.class);
	 	     conf2.setOutputKeyClass(Text.class);
	 	     conf2.setOutputValueClass(Text.class);	
	 	     conf2.setMapperClass(CentroidMapper.class);	 	    
	 	     conf2.setReducerClass(CentroidReducer.class); 	     
	 	     conf2.setInputFormat(TextInputFormat.class);
	 	     conf2.setOutputFormat(TextOutputFormat.class);	
		     FileInputFormat.setInputPaths(conf2, new Path(input + DATA_FILE_NAME));
	 	     FileOutputFormat.setOutputPath(conf2, new Path(centroidOutput));	
	 	     JobClient.runJob(conf2);	
																									
			//Here is the main Job which we will reiterate till we have final clusters		
			String again_input = centroidOutput;
			boolean isdone = false;
			while (isdone == false) 
			{								
				JobConf conf = new JobConf(SeedOptimised_KMeans.class);					 										
				Path hdfsPath = new Path(again_input + OUTPUT_FILE_NAME);
				DistributedCache.addCacheFile(hdfsPath.toUri(), conf);									
				conf.setInt(Dimensions,d);
				conf.setInt(No_Of_Clusters,k);
				conf.setInt(No_Of_Keys,nkeys);
				conf.setJobName(JOB_NAME);
				conf.setMapOutputKeyClass(Text.class);
				conf.setMapOutputValueClass(Text.class);
				conf.setOutputKeyClass(Text.class);
				conf.setOutputValueClass(Text.class);
				conf.setMapperClass(Map.class);
				conf.setReducerClass(Reduce.class);
				conf.setInputFormat(TextInputFormat.class);
				conf.setOutputFormat(TextOutputFormat.class);	
				FileInputFormat.setInputPaths(conf,new Path(input + DATA_FILE_NAME));
				FileOutputFormat.setOutputPath(conf, new Path(output));	
				JobClient.runJob(conf);
				
				if(iteration==4)
					{
						isdone = true;
					}
				++iteration;
				again_input = output;
				output = OUT + System.nanoTime();
			}
		}
}
