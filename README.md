# Mappy
MapReduce, HDFS, and HBase

## Code
The main method is responsible for configuring and initializing the job, setting the number of reducers and defining input and output. 

The map method parses the xml input and converts it to a map. If the "Id" attribute exist, we store the reputation and the original text as pair in a PriorityQueue.

We don't use the TreeMap since there is no unique-garantuee for reputation in the database, meaning that if we used a TreeMap to sort with regards to repyutation entries with identical reputations would overwrite each other. Instead we create a simple generic pair class which have a natural order defined by the Comparable interface. We then combine this with the use of a PriorityQueue to automatically sort our entries. 

THe cleanup method will be called after all key-value pairs have been computed, and will be called once for each file-partition. Here we simply poll the PriorityQueue for the top 10 entries and write the corresponding text to the context with nullable keys. Since we have set the number of reducers to 1 the keys are unnecessary.

In the reducer method we perform something similar to the map method. We parse the text again, extracts Id and Reputation and put these in a PriorityQueue as a pair. We extract the top 10 and write these to the HBase table 'topten' with the Id as the key and write two columns info:id and info:reputation. The resulting table after one run becomes

## Running the program 
We assume that the user has the HBASE and Hadoop environment set up as the lab specifies and that the topten_input folder exist on hdfs with the corresponding users.xml .

## Compile the sources
mkdir topten_classes
javac -cp $HADOOP_CLASSPATH -d topten_classes topten/TopTen.java
jar -cvf topen.jar -C topten_classes/.

## Execute
$HADOOP_HOME/bin/hadoop jar topten.jar topten.TopTen topten_input

------ Output of the application ----
## The output is saved to hbase table topten. If we scan it after a run we get:

ROW                                                          COLUMN+CELL                                                                                                                                                                      
 108                                                         column=info:id, timestamp=1536305084537, value=108                                                                                                                               
 108                                                         column=info:reputation, timestamp=1536305084537, value=2127                                                                                                                      
 11097                                                       column=info:id, timestamp=1536305084537, value=11097                                                                                                                             
 11097                                                       column=info:reputation, timestamp=1536305084537, value=2824                                                                                                                      
 21                                                          column=info:id, timestamp=1536305084537, value=21                                                                                                                                
 21                                                          column=info:reputation, timestamp=1536305084537, value=2586                                                                                                                      
 2452                                                        column=info:id, timestamp=1536305084537, value=2452                                                                                                                              
 2452                                                        column=info:reputation, timestamp=1536305084537, value=4503                                                                                                                      
 381                                                         column=info:id, timestamp=1536305084537, value=381                                                                                                                               
 381                                                         column=info:reputation, timestamp=1536305084537, value=3638                                                                                                                      
 434                                                         column=info:id, timestamp=1536305084537, value=434                                                                                                                               
 434                                                         column=info:reputation, timestamp=1536305084537, value=2131                                                                                                                      
 548                                                         column=info:id, timestamp=1536305084537, value=548                                                                                                                               
 548                                                         column=info:reputation, timestamp=1536305084537, value=2289                                                                                                                      
 836                                                         column=info:id, timestamp=1536305084537, value=836                                                                                                                               
 836                                                         column=info:reputation, timestamp=1536305084537, value=1846                                                                                                                      
 84                                                          column=info:id, timestamp=1536305084537, value=84                                                                                                                                
 84                                                          column=info:reputation, timestamp=1536305084537, value=2179                                                                                                                      
 9420                                                        column=info:id, timestamp=1536305084537, value=9420                                                                                                                              
 9420                                                        column=info:reputation, timestamp=1536305084537, value=1878                                                                                                                      
10 row(s) in 0.1920 seconds


The output is not ordered based on the reputation here because the HBase table orders the rows based on the keys.

