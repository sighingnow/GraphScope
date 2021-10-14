# JAVA SDK
Java SDK is subproject under GraphScope,
presenting a Java SDK for GraphScope analytical engine.
Powered By alibaba-FastFFI, which bridging the huge programming Gap
Between Java and C++, Java PIE SDK enables Java Programmers to acquire 
the following abilities

- **Ease of developing graph algorithms in Java**. 
  
  PIE SDK mirrors 
  the full-featured grape framework, including Fragment, MessageManager, e.t.c.
  Armed with PIE SDK, a Java Programmer can develop algorithms 
  in grape PIE model with no efforts.
- **Efficient execution for Java graph algorithms**. 
  
  There are a bunch of
  graph computing platforms developed in Java, providing  programming interfaces
  in java. 
  But as Java, the language itself, lacks the ability to access low-level system
  resources and memories, their performance is less efficient when compared 
  to C++ framework, grape.
  To provide efficient execution for java graph program, simple JNI bridging 
  is never enough. By leveraging the JNI acceleration provoided by LLVM4JNI,
  PIE SDK substantially narrows the gap between java app and c++ app. As experiments
  shows, the minimum gap is around 1.5x.
  
- **Seamless integration with GraphScope**. 
  
  To run a Java app developed with PIE SDK,
  the user just need to pack Java app into ```jar``` and submit in python client, as 
  show in example. The input graph can be either property graph or projected graph
  in graphscope, and the output can redirected to client fs, vineyard just like normal
  graphscope apps.

# About PIE SDK
## Structure
- grape
## How PIE SDK works

# How to use

## From Maven repo

## Build from local
## Simple Examples

# Performance Metrics





