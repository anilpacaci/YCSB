<!--
Copyright (c) 2015 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

# REST Endpoint Driver for YCSB
This driver enables YCSB to work with databases accessible via the JDBC protocol.

## Getting Started
### 1. Start your database

### 2. Set up YCSB

### 3. Configure your database and table.

### 6. Running a workload
Before you can actually run the workload, you need to "load" the data first.

```sh
bin/ycsb load jdbc -P workloads/workloada -P db.properties -cp mysql-connector-java.jar
```

You can run the workload:

```sh
bin/ycsb rest jdbc -P workloads/workloada -P db.properties -cp mysql-connector-java.jar
```

## Configuration Properties

```sh
db.driver=com.mysql.jdbc.Driver				# The JDBC driver class to use.
db.url=jdbc:mysql://127.0.0.1:3306/ycsb		# The Database connection URL.
db.user=admin								# User name for the connection.
db.passwd=admin								# Password for the connection.
jdbc.fetchsize=10							# The JDBC fetch size hinted to the driver.
jdbc.autocommit=true						# The JDBC connection auto-commit property for the driver.
```

Please refer to https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties for all other YCSB core properties.
