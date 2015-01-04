#CoffeeTable (current version -- 1.5)
###(c) Taylor G Smith

*DataTable structure in Java*

At a high level, this library provides a data structure class (DataTable) similar to that of the DataTable in C# and implementing vector operations and functionality similar to that of the R data.frame.  Additionally, the library provides the tools to easily create new DataTables from various types of delimited files as well as to write DataTables to files, serialize and deserialize from ObjectOutputStreams.

By nature, a DataTable’s columns are type safe, while its rows allow wildcard adds. However, a special class, TheoreticalValue (similar to C# DBNull), will replace empty fields, NA fields and infinite values, allow for easy arithmetic functions while avoiding NullPointerExceptions, and otherwise stay out of the way of your operations. 

The library also provides an extensible set of APIs for DataTable's super class, SchemaSafeDataStructure, RenderableSchemaSafeDataStructure and AbstractDataTable, that provides a framework for a lightweight DataTable without any added "fluff." While the DataTable seeks to replicate an R-style dataframe, its superclass merely enforces typesafety to adds in a grid structure of DataColumns and DataRows.

*NEW* as of version 1.5: a Matrix class that extends AbstractDataTable, which enforces all adds be numeric. Also supports matrix transposes, mathematics, etc.

*The documentation for this library is provided in a JavaDoc included in this Git*

This library was built and tested in Java SE 1.6

For bug reports, future implementation ideas or general commentary, please email me at tgsmith61591@gmail.com