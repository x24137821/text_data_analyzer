
first of all we need to create 3 ec2 instances, one is master instance and others worker nodes
you also need to create a kinesis data stream
ssh into the instances
master node has 7 scripts and they will run on virtualenv named as piper
you need to activate it 
then you will run the scripts in the order

kinses_read.py which will show that is readable coming for ingestion and kinesis

kinesis_consumer.py which counts the words and do sentiment analysis

sliding_window.py which will give you last 5 minutes trending words

hybrid_parallelism.py which splits the process into 2 parts and the worker nodes process them

Parallel.py and sequential.py generates two csv files which contains the benchmarks of parallel process and sequential process

final_plot.py this will generate graphs by using generated csv files

