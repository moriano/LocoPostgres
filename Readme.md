# LocoPostgres

LocoPostgres is a JDBC driver for PostgreSQL (and also compatible with 
amazon Redshift). 

The only dependency that LocoPostgres has while running is Log4j, it uses
other dependencies but just for testing. 

Should you use LocoPostgres in production? No, period. I wrote this driver
mainly out of curiosity, during my job i became quite familiar with the 
PostgreSQL protocol and decided that it would be interesting to write a 
JDBC driver just for fun. As there are no external dependencies, the driver 
is a good way to understand the PostgreSQL protocol. Also the driver logs 
every single packet that is sent and received from the server. 

## Usage

To use loco postgres you will first have to register the driver. To do so do 

`/tmp/locopostgres-debug.log`

# FAQ

### 1. What is LocoPostgres?

LocoPostgres is a JDBC driver for PosgreSQL database. It also works with AWS Redshift, as 
Redshift uses the same protocol. 

### 2. Why should I use LocoPostgres instead of the official PostgreSQL driver?

You should not! If you want to connect to PostgreSQL or Redshift you should either use the 
official PostgreSQL driver or the RedshiftJDBC driver (in the case of Redshift). LocoPostgres 
is, by no means, a replacement for those.

### 3. Why did you write LocoPostgres?

Mainly due to curiosity, but also literally because I can :). 

As part of my job I ended up learning the PostgreSQL protocol and I wonder whether I could
write a JDBC driver just for fun.

### 4. What makes LocoPostgres special?

I have decided to use NO dependencies in this code except Log4j. This means that I am not even 
using some of the basic Apache Commons libraries. The reason for this is that I want to simply 
write everything from scratch. Again, this is just for fun, it is not practical.

I have allowed myself to use libraries for the unit tests, but not for the main driver.

The other interesting part of this driver is that I have tried to make the code easy to read, I 
have added plenty of comments around. Also the driver spits out the protocol packets into the logs, 
this makes this driver very convenient if you want to simply study the protocol.


## Usage

To establish a connection you must use a url that looks like `jdbc:loco:postgresql` instead of `jdbc:postgresql`, 
this is actually not compliant with the actual PostgreSQL protocol, and it is done on purpose. Essentially I am trying 
to prevent anybody from using LocoPostgres in a production environment by mistake.

## Code structure

* LocoDriver: The class implementing `Driver`, it is in charge of establishing the 
connection to the server.
* Packet: The main abstraction representing network packets being sent and received 
from the server. 
* LocoNetwork: The object in charge of opening the actual TCP Connection to the server to
send and receive network packets. This is done with a Java Socket.
* LocoStatement: The object implementing the `Statement` interface.
* LocoResultSet: The object implementing the `ResultSet` interface.
* LocoConnection: The object implementing the `Connection` interface.


## Roadmap

Next steps: 

* [DONE] Implement unit tests using the actual PostgreSQL JDBC driver as a `reference` or `valid` way to get data from 
the database and compare the results with LocoPostgres
* [DONE] Improve documentation (specially javadocs)
* [DONE] Implement the read of the result set
* [DONE] Implement batch methods for statement 
* [DONE] Implement query execution cancel
* [DONE] Pass all the unit tests
    * Regression, as more tests are added
    * Provided a base method so we can have tests with actual data in a reusable manner
* [WIP] read operations.
    * Identified issues when reading ts tz ranges
    * [DONE] Identified issue while reading arrays of bytes
* Implement authentication methods other than md5
    * [DONE] Implement clear text password
    * Implement SCRAM SHA 256 
* Implement prepared statements, and the parse-bind-execute packets in the postgres protocol
    * [DONE] Prepared statement, no parameters
    * Prepared statement parameters
        * int, float, double, short
        * strings
        * boolean
    
* Fully implement the Statement interface

