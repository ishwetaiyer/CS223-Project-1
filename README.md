# CS-223-Transaction-Processing-
Transaction Processing and Distributed Data Management Project

# Getting Started
The below instructions will get you a copy of the project up and running on your local machine for development and testing purposes:
1.Clone the GitHub repo using the command git clone 'Project SSH'
2.Open the project in IntelliJ/Eclipse/any other IDE.
3.Maven - Dependency Management
4.Add concurrency queries file to the resources folder.
5.Right click on resources folder and select "mark directory root" as "sources root"

# Preparing the workload
The workloads for each workload is given in three files. These files were combined in a single large file, using GNU tools (sed and awk) and vim. Each line is of the following format:'<Timestamp>', <Statement>, where statement can be an insert or a select.

# Run Instructions:
We use maven as a build tool. Run the following commands from your src directory:
`mvn package`
`mvn exec:java`
The simulation can be configured by parameters in "application.properties" file.

# Viewing the result
For both mysql and postgres, the result is stored in a database specified by "logging" parameter in application.properties. There are two tables in that db: `simulation_logging` (which contains global running information) and `query_logging` (which contains information for each statement of SELECT type).

Authors
Anupriya Prasad
Aditya Harit
Shweta Iyer


