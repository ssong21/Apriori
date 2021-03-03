/*
 * MRMiner.java
 * 
 * Copyright 2019 Scott Song 21 <ssong21@hdp2-desktop.amherst.edu>
 */

import java.io.*;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.*;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.lang.Integer;
import org.apache.hadoop.io.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class MRMiner {
	
	public static class MRMapperR1 //DO MULTIPLE .SPLIT("\n"); .SPLIT(" ");
       extends Mapper<Object, Text, Text, Text>{
       
	public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	 //Please make it very very very evident with comments and so on where this hard coding is happening.
	 // Hardcode the TOTAL # of transactions: 103119400

	  //System.out.println("START OF MAP");
	  //generate all transactions as hashset of hashsets
	  ArrayList<HashSet<Integer>> allTransactions = new ArrayList<HashSet<Integer>>();
	  String tempValue = value.toString();
	  String[] stringTransactions = tempValue.split("\\r?\\n");
	  //System.out.println("stringTransactions.length(): " + stringTransactions.length);
	  for (int i = 0; i < stringTransactions.length; i++){
		  String[] tempTransaction = stringTransactions[i].split("\\s+");
		  HashSet<Integer> containerInt = new HashSet<Integer>();
		  for (int d = 0; d < tempTransaction.length; d++){
			  containerInt.add(Integer.parseInt(tempTransaction[d]));
		  }
		  allTransactions.add(containerInt);
	  }
	  //System.out.println("allTransactions.size(): " + allTransactions.size());
	  int minsupp = Integer.parseInt(context.getConfiguration().get("supp"));
	  int corr = Integer.parseInt(context.getConfiguration().get("corr"));
	  double totalTransactions = 2000;
	  double frac = (double)allTransactions.size()/totalTransactions; // CHECK THIS FRACTION
	  int mineSupp = (int)((minsupp-corr)*frac); //check this
	  
	  //Generating the (n=1) frequent itemsets
	  HashMap<HashSet<Integer>,Integer> candidates = new HashMap<HashSet<Integer>,Integer>();
	  for (HashSet<Integer> transaction : allTransactions){
		  for (Integer item : transaction){
			  HashSet<Integer> holder = new HashSet<Integer>();
			  holder.add(item);
			  if (!candidates.containsKey(holder)){
				  candidates.put(holder,1);
			  }
			  else{
				  candidates.put(holder,candidates.get(holder)+1);
			  }
		  }
	  }
	  //Filtering n=1 itemsets, results in frequent n=1 itemsets
	  Iterator it = candidates.entrySet().iterator();
	  while (it.hasNext()){
		  Map.Entry<HashSet<Integer>,Integer> pair = (Map.Entry<HashSet<Integer>,Integer>)it.next();
		  int valu = pair.getValue();
		  if (valu < mineSupp){
			  it.remove();
		  }
	  }
	  //Writing n=1 frequent itemsets
	  for (HashSet<Integer> itemTrans : candidates.keySet()){
		  String itemList = "";
		  for (Integer item : itemTrans){
			  itemList = itemList + item + " ";
		  }
		  Text itemset = new Text(itemList.trim());
		  context.write(itemset,new Text("1"));
	  }
	  boolean finished = false;
	  int k = 2;
	  while (!finished){
		  HashMap<HashSet<Integer>,Integer> nextCand = new HashMap<HashSet<Integer>,Integer>();
		  HashSet<HashSet<Integer>> notCand = new HashSet<HashSet<Integer>>();
		  for (HashSet<Integer> setA : candidates.keySet()){
			  for (HashSet<Integer> setB : candidates.keySet()){
				  if (setA.equals(setB)){
					  continue;
				  }
				  HashSet<Integer> copy = new HashSet<Integer>();
				  copy.addAll(setA);
				  copy.addAll(setB);
				  HashSet<Integer> combined = copy;
				  if (combined.size() == k && !nextCand.containsKey(combined) && !notCand.contains(combined)){
					  boolean found = true;
					  for (Integer e : combined){
						  HashSet<Integer> newCopy = new HashSet<Integer>();
						  newCopy.addAll(combined);
						  newCopy.remove(e);
						  HashSet<Integer> removedE = newCopy;
						  if (!candidates.containsKey(removedE)){
							  found = false;
							  break;
						  }
					  }
					  if (found){
						nextCand.put(combined,0);
					  }
					  else{
						notCand.add(combined);
					  }
				  }
			  }
		  }
		  if (nextCand.isEmpty()){
			  finished = true;
		  }
		  else{
			  candidates = nextCand;
			  //Count candidate support
			  for (HashSet<Integer> transaction : allTransactions){
				  for (HashSet<Integer> validCand: candidates.keySet()){
					  if (transaction.containsAll(validCand)){
						  candidates.put(validCand,candidates.get(validCand)+1);
					  }
				  }
			  }
			  //Filtering
			  Iterator innerIt = candidates.entrySet().iterator();
			  while (innerIt.hasNext()){
				Map.Entry<HashSet<Integer>,Integer> pair = (Map.Entry<HashSet<Integer>,Integer>)innerIt.next();
				int val = pair.getValue();
				if (val < mineSupp){
					innerIt.remove();
				}
			  }
			  //Writing
			  for (HashSet<Integer> list : candidates.keySet()){
				  String itemList = "";
				  for (Integer item : list){
					  itemList = itemList + item + " ";
				  }
				  Text itemset = new Text(itemList.trim());
				  context.write(itemset,new Text("1"));
			  }
			  k++;
		  }
	 }
  }
}
  public static class MRMapperR2
       extends Mapper<Object, Text, Text, IntWritable>{
	private Path[] localFiles;
    private Path stopPath;
    private HashSet<HashSet<Integer>> storedSets = new HashSet<HashSet<Integer>>();
    private boolean terminate;
		   
	public void setup(Context context){
		Configuration conf = context.getConfiguration();
        try {
			FileSystem sys = FileSystem.getLocal(conf);
			localFiles = DistributedCache.getLocalCacheFiles(conf);																		
			BufferedReader readSet = new BufferedReader(new InputStreamReader(sys.open(localFiles[0])));
			String set = readSet.readLine();
			while(set != null){
				String[] splitSet = set.trim().split("\\s+");
				HashSet<Integer> containerInt = new HashSet<Integer>();
				for (int d = 0; d < splitSet.length; d++){
				  containerInt.add(Integer.parseInt(splitSet[d]));
				}
				storedSets.add(containerInt);
				set = readSet.readLine();
			}
		}
		catch (IOException e){
			terminate = true;
			e.printStackTrace();
			return;
		}
	}

	//0 = R, 1 = S
	public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
     if (storedSets.isEmpty()){
		 setup(context);
	 }
	 //System.out.println("storedSets.size(): " + storedSets.size());
	 //generate all transactions as arraylist of arraylists
	 String tempValue = value.toString();
	 String[] stringTransactions = tempValue.split("\\r?\\n");
	 ArrayList<HashSet<Integer>> allTransactions = new ArrayList<HashSet<Integer>>();
	 for (int i = 0; i < stringTransactions.length; i++){
		  String[] tempTransaction = stringTransactions[i].split("\\s+");
		  HashSet<Integer> containerInt = new HashSet<Integer>();
		  for (int d = 0; d < tempTransaction.length; d++){
			  containerInt.add(Integer.parseInt(tempTransaction[d]));
		  }
		  allTransactions.add(containerInt);
	 }
	 //System.out.println("allTransactions.size(): " + allTransactions.size());
	 HashMap<HashSet<Integer>,Integer> freqCount = new HashMap<HashSet<Integer>,Integer>();
	 for (HashSet<Integer> freqSet : storedSets){
		 freqCount.put(freqSet,0);
	 }
	 
	 for (HashSet<Integer> transaction : allTransactions){
		 for (HashSet<Integer> checkSet : freqCount.keySet()){
			 if (transaction.containsAll(checkSet)){
				 freqCount.put(checkSet,freqCount.get(checkSet)+1);
			 }
		 }
	 }

	 for (HashSet<Integer> checkSet : freqCount.keySet()){
		 String itemList = "";
		 for (Integer item : checkSet){
			itemList = itemList + item + " ";
		 }
		 Text textItemSet = new Text(itemList.trim());
		 IntWritable count = new IntWritable(freqCount.get(checkSet));
		 context.write(textItemSet,count);
	 } 
    }
  }
  
  public static class MRReducerR1
       extends Reducer<Text,Text,Text,NullWritable> {

    public void reduce(Text mark, Iterable<Text> pairs,
                       Context context
                       ) throws IOException, InterruptedException {
	  context.write(mark,NullWritable.get());
    }
  }
	
	public static class MRReducerR2
       extends Reducer<Text,IntWritable,Text,IntWritable> {

    public void reduce(Text mark, Iterable<IntWritable> count,
                       Context context
                       ) throws IOException, InterruptedException {
	  int counter = 0;
      for (IntWritable num : count) {
		counter = counter + num.get();
      }
      //System.out.println("here: " + mark.toString() + "counter: " + counter);
      int minsupp = Integer.parseInt(context.getConfiguration().get("supp"));
	  int corr = Integer.parseInt(context.getConfiguration().get("corr"));
      if (counter >= (minsupp-corr)){
		context.write(mark, new IntWritable(counter));
	  }
	}
}
	
	public static void main (String args[]) throws Exception {
		Configuration conf1 = new Configuration();
		conf1.set("supp", args[0]);
		conf1.set("corr", args[1]);
		Job job1 = Job.getInstance(conf1, "MR Miner: Round 1");
		job1.setInputFormatClass(MultiLineInputFormat.class); //file must be in same folder, checckkk
		org.apache.hadoop.mapreduce.lib.input.NLineInputFormat.setNumLinesPerSplit(job1, Integer.parseInt(args[2]));
		job1.setJarByClass(MRMiner.class);
		job1.setMapperClass(MRMapperR1.class);
		FileInputFormat.addInputPath(job1, new Path(args[3]));
		job1.setReducerClass(MRReducerR1.class);
		FileOutputFormat.setOutputPath(job1, new Path(args[4]));
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);


		if (job1.waitForCompletion(true)){
			Configuration conf2 = new Configuration();
			Path combinedOut = new Path(new Path(args[4]), "part-r-00000");
			DistributedCache.addCacheFile(combinedOut.toUri(),conf2);
			conf2.set("supp", args[0]);
			conf2.set("corr", args[1]);
			Job job2 = Job.getInstance(conf2, "MR Miner: Round 2");
			job2.setInputFormatClass(MultiLineInputFormat.class);
			org.apache.hadoop.mapreduce.lib.input.NLineInputFormat.setNumLinesPerSplit(job2, Integer.parseInt(args[2]));
			job2.setJarByClass(MRMiner.class); //might mess things up
			job2.setMapperClass(MRMapperR2.class);
			FileInputFormat.addInputPath(job2, new Path(args[3])); 
			job2.setReducerClass(MRReducerR2.class);
			FileOutputFormat.setOutputPath(job2, new Path(args[5]));
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
		else{
			System.out.println("first job failure");
			//System.exit(job1.waitForCompletion(true) ? 0 : 1);
		}
  }
}


