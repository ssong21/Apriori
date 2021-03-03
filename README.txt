READ ME:

IMPORTANT WARNING:

To properly run MRMiner, MultiLineInputFormat MUST been in the same directory as MRMiner

IMPORTANT WARNING:

MRMiner currently is hardcoded to work on input data files of length 2000 transactions. To work with other lengths, change LINE 65 in MRMiner. 

LINE 65: double totalTransactions = 2000;



To compile the MRMiner file, run the following command:

hadoop com.sun.tools.javac.Main MRMiner.java MultiLineInputFormat.java

To create the jar, run the following command:

jar cf mr.jar MRMiner*.class MultiLineInputFormat*.class

To run the program, exceute the following command:

hadoop jar mr.jar MRMiner MINSUPP CORR TRANS_PER_BLOCK PATH_TO_INPUT PATH_TO_FIRST_OUT PATH_TO_FINAL_OUT


Arguments: 

MINSUPP, an integer, is the minimum support threshold.
CORR, an integer, is the “correction” to the minimum support threshold: the first Map function will mine the set of transactions it receives with a minimum support threshold of MINSUPP-CORR.
TRANS PER BLOCK, an integer, is the number of transactions per “block” of the dataset, which are simultaneously given in input to the first Map function.
PATH TO INPUT is the path to the HDFS input directory containing the input dataset.
PATH TO FIRST OUT is the path to the HDFS output directory for the first round.
PATH TO FINAL OUT is the path to the HDFS final output directory.



