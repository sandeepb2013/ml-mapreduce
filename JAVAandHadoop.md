# Introduction #

This is a class project which aims to demonstrate a simple Proof of Concept about massive parallelization of Machine learning algorithms on a Distributed Fie System. For the purposes of proof of concept we use the Hadoop DFS and its JAVA API. The first fully functional algorithm to be implemented was the Logistic Regression using Iterative Newton raphson. This page serves as a general purpose road map and experiences in designing with hadoop and


# Details #
**The pain**
Frankly speaking I am a little upset at the amount of basic java code it takes to get a single algoritm to run. The boilerplate JAVA code that has to be written for even the simplest proof of concepts is mind boggling. But why use JAVA then? Most people rather avoid the pain and use python or ERLang. And its not like we are masochists. I have looked at options like cascalog and clojure based systems for hadoop as well. For even a moderately complex system the config files will get huge and complex no matter what language you use! Facing the reality you come to understand the obviousness of the situation!

**Possible solution?**
I was thinking of a visual tool to design the entire system as a block diagram and let the visual tool create the conf file for you. The big advantage of this would be that the developer spends 90% of his time programming the mapper/reducers rather than wasting a lot of it in just changing the configuration for testing/debugging. It is just a  technical thingy to do this but still very important for efficient testing and debugging.

**The cool, the uncool and the pathetic**
The coolest part of designing with MR is fitting your algorithm into a MR paradigm in a way that you can achieve much better time and space tradeoff. But once this is done the worst part is to actually code in all the mappers and reducers. It is monotonus and I like to get it over in a day if I can! Long run cycles to test your algorithms can be such mood killers! So test with small data and atleast 2 Gigs RAM if you want to live.

**Debugging and testing(on single machines)**
Always test with small data and have a quick and dirty MATLAB/python model in place to verify the results. That is how I did it for Machine learning algorithms. This saves time. Initial development required coding in a lot of utilities like reading and writing commmon global files to the HDFS. Linear algebra stuff etc... Small test benches to test these API's is the best idea. Create a few test benches with dummy MR classes with a little data, to unit test your utilities. One must learn to use a log4j logger to hunt down bugs.

**UPDATE**
There is now an Apache effort called mahout that was started just after I finished this project which has really taken shape and bloomed. Would like to have a look at that too.