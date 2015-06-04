# MongoBug
This program was created after my team found what appears to be a bug in the MongoDB load balancer with the WiredTiger storage engine. This program will simply start up a number of threads and then insert documents into MongoDB.

The program can be run like:
Usage: MongoBugMain -h <arg> -p <arg> -d <arg> -c <arg> -t<arg> -i <arg> -m arg
    A utility to insert many documents into MongoDB in a multi-threaded manner
    All arguments are optional
Arguments:
  -h  --host <arg>              The host for the mongos, default: localhost
  -p  --port <arg>              The port for the mongos, default: 27017
  -d  --database <arg>          The database name, default: mongobug
  -c  --collection <arg>        The collection name, default: lostdata
  -t  --threadcount <arg>       The number of threads to start, default: 10
  -i  --insertsperthread <arg>  The number of inserts per thread, default: 40000
  -m  --message <arg>           The message inserted for this run, default: default message
      --help                    Prints this message
