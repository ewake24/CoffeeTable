#CoffeeTable
###(c) Taylor G Smith

*DataTable structure in Java*

This library provides a data structure class (DataTable) similar to that of the DataTable in C# and implementing vector operations and functionality similar to that of the R data.frame.  Additionally, the library provides the tools to easily create new DataTables from various types of delimited files as well as to write DataTables to file. 

By nature, a DataTable’s columns are type safe, while its rows allow wildcard adds. However, a special class, MissingValue (similar to C# DBNull), will replace empty fields or NA fields, allow for easy arithmetic functions while avoiding NullPointerExceptions, and otherwise stay out of the way of your operations.

For bug reports, future implementation ideas or general commentary, please email me at tgsmith61591@gmail.com