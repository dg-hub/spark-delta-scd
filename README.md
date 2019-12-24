# Spark Delta SCD
A Scala Package for automating Slowly Changing dimensions via Delta Lake storage format

See Delta Lake website for more information https://delta.io/

# License 
MIT - Copyright (c) 2019 Daniel G - https://github.com/dg-hub

# Features

* **createDeltaTable()** -  Creates a target table with attribute columns from DataFrame
* **optimise()** -  Reduce target Delta data to 1 (numFiles)
* **execute()** - Executes a SCD merge into target path

# Feature requests
##### Please use Git Hub issues to request new features:
https://github.com/dg-hub/spark-delta-scd/issues


# Release Notes

Version 1.8 (December 24, 2019) Add initial code to build and execute Merge into target

# Maven Dependency
```xml
<dependency>
  <groupId>nz.co.glidden</groupId>
  <artifactId>spark-delta-scd</artifactId>
  <version>1.8</version>
</dependency>
```

# Usage

##### Include the libary from Maven Central:
```shell
bash> spark-shell --packages nz.co.glidden:spark-delta-scd:1.8
````

##### Create a Target Delta Table
```scala
scala> Scd2.createDeltaTable(source_dataframe,"/tmp/delta-target-location",true)
```
##### Execute SCD Merge into Target Delta Table 
```scala
scala> Scd2.execute(updates_dataframe,"/tmp/delta-target-location",Seq("id"),true)
```
##### Read the target Table
```scala
scala> spark.read.format("delta").load("/tmp/delta-target-location").show()
```
