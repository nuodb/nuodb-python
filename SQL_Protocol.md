# NuoDB SQL Protocol
This document describes the protocol between SQL clients and Transaction Engines in NuoDB. With the exception of an initial handshake, all messages are tightly encoded in a binary representation and encrypted.

Note that the protocol is designed to handle revisions and be compatible across versions. Even before the first public release of the product several revisions were made. This document covers revisions from Protocol Version 10 to Protocol Version 11.

When the protocol is updated this document should also be updated to reflect those changes.

The following NuoDB releases included changes to the SQL Protocol Version:

NuoDB Version  | SQL Protocol Version
-------------- | --------------------
1.0            | 10                  
1.0.2          | 11                  

## Initial Session Handshake
All exchanges between client and server follow the same pattern. An SQL client (client) initiates a session by sending a clear-text XML message to a Transaction Engine (server). 
The client then follows this request with a binary message to access the database through an authentic, confidential session. The server responds, both sides compute a session key, and then a third message is sent by the client to verify its credentials.

The client opens a TCP connection to the server, and sends:

```xml
<Connect Service=”SQL2”>
  
[connection_protocol=”0” Thread=”0”]
```

The client sends OpenDatabase (see below) and the server responds. Both sides compute the session key, and from this point forward the session is encrypted.

The client sends Authentication and the server responds. If both sides have agreed on the same session key then negotiation is complete and the session is ready for handling SQL requests.

The session remains open until either side explicitly closes the connection, or until the client sends `CloseConnection` to the server.

## General Request/Response Formatting
Once the session is established, all request messages from the client start with a number that identifies an operation. The payload that follows is specific to that operation.

In most valid exchanges the first field in the response is the integer value zero, meaning that there was no error, followed by any additional (optional) response details. In the case of a closed connection no response is sent from the server. In the case of some unexpected, internal error the connection is closed with no response.

If a client request is malformed, or contains invalid SQL, then the response from the server will start with a non-zero SQL error code, and be followed by the associated error message and a SQL exception state message. The connection should always be left open after such an error message.

The next two sections explain how data types are identified and encoded, and how the specific request messages are formatted.

## Datatypes
All fields in a request or response are encoded using a tight, simple set of rules. Each field starts with a single byte that identifies the data type and how the associated value is encoded. For instance, if a field starts with 53 it is identifying an integer value that is encoded using the following two bytes in the message. In other words, a 16-bit integer value follows the field header.

For some common values the initial identifier encodes both the type and the value. For instance, if a field starts with 2 then it identifies a boolean, and also specifies that the boolean value is True. No additional data is encoded.

Here are the data types, and their associated encoding. Because there is more than one way to encode a given type, they are broken into the groups are used in the Protocol Messages section. For instance, when a Protocol Message specifies that it uses an Integer, that field and value can be encoded using any of the forms described below for Integers.

#### Unique Types


Field Header | Field Type & Format
------------ | --------------------
1            | Null                  
2            | Boolean with value `True`      
3            | Boolean with value `False`      
200          | 32-byte UUID      
199          | Fixed point integer with scale  
225          | Fixed point integer with scale      

#### Integer Types

Field Header | Field Type & Format
------------ | --------------------
10 - 19      | The Integer values `-10` to `-1` (respectively)                  
20 - 51      | The Integer values `0` to `31` (respectively)      
52 - 59      | A signed integer value encoded using the next 1 to 8 bytes (respectively)    

E.g., a field that starts with the identifier 21 represents the Integer value 1, and has no further payload. A field that starts with 52 is followed by a single byte that encodes an 8-bit signed integer value.

#### Scaled Integer Types

Field Header | Field Type & Format
------------ | --------------------
60 - 68      | A scaled integer value of length 0 to length 8 bytes (respectively). The format is one byte giving the scale followed by 0 to 8 bytes with the scaled integer value.

#### Double Precision Types

Field Header | Field Type & Format
------------ | --------------------
77 - 85      | IEEE double-precision floating point value of length 0 to length 8 bytes (respectively)

#### String (UTF8) Types

Field Header | Field Type & Format
------------ | --------------------
69 - 72      | A string where the first 1 to 4 bytes (respectively) encode an integer that is the number of bytes that follow as the string value                 
109 - 148    | A string of length 0 to 39 bytes (respectively)     

All strings are UTF-8 encoded lengths of bytes, with a header that specifies the number of bytes in the string. For instance, a field starting with the identifier 70 will be followed by two bytes that encode a 16-bit integer value. That value is the length of string. A field starting with 112 will be followed by a UTF-8 string three bytes long.

#### Opaque Types

Field Header | Field Type & Format
------------ | --------------------
73 - 76      | An opaque value where the first 1 to 4 bytes (respectively) encode an integer that is the number of bytes that follow as the value                
149 - 188    | An opaque value of length 0 to 39 bytes (respectively)   

#### BLOB/CLOB Types

Field Header | Field Type & Format
------------ | --------------------
189 - 193    | A BLOB (Binary Large OBject) value where the first 0 to 4 bytes (respectively) encode an integer that is the number of bytes that follow as the binary value                
194 - 198    | A CLOB (Character Large OBject) value where the first 0 to 4 bytes (respectively) encode an integer that is the number of bytes that follow as the character string 

#### Time Types

Field Header | Field Type & Format
------------ | --------------------
86 - 94      | Milliseconds since the epoch of length 0 to length 8 bytes (respectively)                
95 - 103     | Nanoseconds since the epoch of length 0 to length 8 bytes (respectively) 
104 - 108    | Milliseconds since midnight of length 0 to length 4 bytes (respectively)

#### Scaled Time Types

Field Header | Field Type & Format
------------ | --------------------
209 - 216    | A scaled time value of length 0 to length 8 bytes (respectively). The format is one byte giving the scale followed by 0 to 8 bytes with the scaled time value.               
217 - 224    | A scaled timestamp value of length 0 to length 8 bytes (respectively). The format is one byte giving the scale followed by 0 to 8 bytes with the scaled timestamp value.

#### Scaled Date Types

Field Header | Field Type & Format
------------ | --------------------
201 - 208    | A scaled date value of length 0 to length 8 bytes (respectively). The format is one byte giving the scale followed by 0 to 8 bytes with the scaled date value.

## Protocol Messages

Each of the messages below is broken into the expected, valid input and the resulting response message, or no specific response if the default response is all that’s expected. The responses shown below are for the valid cases only. Error responses are described in the previous section on general formatting.

For each message the name of the message is followed by the identifier for that message. The message identifier will always be encoded as the first byte in any any request from a client to a server.

### Analyze (70)

_Request_      | _Response_ 
-------------- | ----------------
Integer (mask) | String (result) 

### Authentication (86)

_Request_      | _Response_ 
-------------- | -------------------
Integer (mask) | String ("Success!") 

This message is sent exactly once, after the the initial connection is made and the database is opened. After calculating the session key the client encrypts a known string and sends it to the server. If the server can decrypt the message and get the expected string then both sides have agreed on the same key. Otherwise, the session is closed by the server.

### CloseConnection (5)

_Request_ | _Response_ 
--------- | ----------
None      | None 

### CloseResultSet (28)

_Request_                    | _Response_ 
---------------------------  | ----------
Integer (result set handle)  | None 

### CloseStatement (15)

_Request_                    | _Response_ 
---------------------------  | ----------
Integer (statement handle)   | None 

### CommitTransaction (7)

_Request_ | _Response_ 
--------- | ------------------------------------------------------------------------------------
None      | 64-bit Integer (transaction id)
          | Integer (node id)
          | 64-bit Integer (commit sequence) 

### CreateStatement (11)

_Request_                    | _Response_ 
---------------------------  | --------------------------
None                         | Integer (statement handle) 

If auto-commit is true on this connection, then creating a new statement causes any existing transaction to commit.

### Execute (18)

_Request_                  | _Response_ 
-------------------------- | ----------------------------------------------------------
Integer (statement handle) | EXECUTE RESPONSE (1 if execution is finished, 0 otherwise) 
String (statement)         |

Blocks pending commit

### ExecuteBatchStatement (83)

_Request_                  | _Response_ 
-------------------------- | ----------------------------------------------------------
Integer (statement handle) | COUNT Integers (update count) 
Integer (count)            | 64-bit Integer (transaction id)
COUNT Strings (statement)  | Integer (node id)
                           | 64-bit Integer (commit sequence)

Blocks pending commit

In the case of failed execution, this will return the specific error message:

Integer (EXECUTE_FAILED = -3), Integer (SQL error code), String (error message)

### ExecuteBatchPreparedStatement (84)

_Request_                              | _Response_ 
-------------------------------------  | -----------------------------
Integer (prepared statement handle)    | COUNT Integers (update count)
COUNT                                  |

Block pending commit

### ExecuteKeys (91)

_Request_                                          | _Response_ 
---------------------------------------------------| ------------------
Integer (1 == return keys, 2 == don't return keys) | ExecuteResponse(0) 
Integer (statement handle)                         | 
String (execute statement)                         |

### ExecuteKeyIds (93)

_Request_                  | _Response_ 
-------------------------- | ------------------
Integer (key id count)     | ExecuteResponse(0)
Integer (statment handle)  | 
String (execute statement) |

### ExecuteKeyNames (92)

_Request_                  | _Response_ 
-------------------------- | ------------------
Integer (key name count)   | ExecuteResponse(0)
Integer (statment handle)  | 
String (execute statement) |


### ExecutePreparedStatement (22)

_Request_                           | _Response_ 
----------------------------------- | ----------------------------------------
Integer (prepared statement handle) | ExecuteResponse(result, true, proto > 1)
Integer (parameter count)           |

### ExecutePreparedQuery (23)

_Request_                           | _Response_ 
----------------------------------- | -----------------
Integer (prepared statement handle) | ResultSet(result)
Integer (parameter count)           | 

### ExecutePreparedUpdate (24)

_Request_                           | _Response_ 
----------------------------------- | -----------------
Integer (prepared statement handle) | ResultSet(result)
Integer (parameter count)           | 

### ExecuteQuery (19)

_Request_                  | _Response_ 
-------------------------- | -----------------------
Integer (statement handle) | ResultSet(query-result)
String (query)             |

### ExecuteUpdate (20)

_Request_                  | _Response_ 
-------------------------- | -------------------------
Integer (statement handle) | ExecuteResponse(0, false)
String (update string)     |

### ExecuteUpdateKeys (94)

_Request_                                          | _Response_ 
-------------------------------------------------- | -------------------------
Integer (1 == return keys, 2 == don't return keys) | ExecuteResponse(0, false)
Integer (statement handle)                         | 
String (update string)                             |

### ExecuteUpdateKeyIds (96)

_Request_                 | _Response_ 
------------------------- | -------------------------
Integer (key id count)    | ExecuteResponse(0, false)
Integer (statment handle) | 
String (update statement) |

### ExecuteUpdateKeyNames (95)

_Request_                 | _Response_ 
------------------------- | -------------------------
Integer (key name count)  | ExecuteResponse(0, false)
Integer (statment handle) | 
String (update statement) |

### GetAutoCommit (59)

_Request_ | _Response_ 
--------- | -------------------------------------
None      | 0 if not auto-commit, 1 if auto-commit

### GetCatalog (101)

_Request_ | _Response_ 
--------- | -------------------------------------
None      | String (null)	

### GetCatalogs (34)

_Request_ | _Response_ 
--------- | -------------------------------------
None      | ResultSet(database metadata catalogs)

### GetColumns (38)

_Request_                | _Response_ 
------------------------ | --------------------------------
String (catalog pattern) | ResultSet(matching column names)
String (schema pattern)  | 
String (table pattern)   |
String (column pattern)  |


### GetCurrentSchema (102)

_Request_ | _Response_ 
--------- | --------------------
None      | String (schema name)

### GetDatabaseMetaData (33)

_Request_ | _Response_ 
--------- | ---------------------------------------
None      | Integer (1)
          | String (product name) 
          | Integer (2)
          | String (product version)
          | Integer (4)
          | String (database major version)
          | Integer (3)
          | String (database minor version)
          | Integer (5)
          | String (default transaction isolation)
          | Integer (0)

### GetGeneratedKeys (87)

_Request_                  | _Response_ 
-------------------------- | --------------------
Integer (statement handle) | ResultSet (statement generated keys)

### GetImportedKeys (41)

_Request_                  | _Response_ 
-------------------------- | --------------------
String (catalog pattern)   | ResultSet (matching imported keys)
String (schema pattern)    |
String (table pattern)     |

### GetIndexInfo (43)

_Request_                  | _Response_ 
-------------------------- | --------------------
String (catalog pattern)   | ResultSet (matched index info)
String (schema pattern)    |
String (table pattern)     |
Integer (unique flag)      |
Integer (approximate flag) |

If the unique flag is non-zero then the index is unique.

If the approximate flag is non-zero then the index is approximate.

### GetMetaData (26)

_Request_                   | _Response_ 
--------------------------- | --------------------
Integer (result set handle) | Integer (column count)
                            | *Column Response*
                            
For each column in column count the column response is as follows:

* String (catalog name)
* String (schema name)
* String (table name)
* String (column name)
* String (column label)
* String (collation sequence)
* String (column type name)
* Integer (column type)
* Integer (column display size)
* Integer (precision)
* Integer (scale)
* Integer (flags: bit-wise or of any RSMD flags)

### GetParameterMetaData (85)

_Request_                           | _Response_ 
----------------------------------- | --------------------
Integer (prepared statement handle) | Integer (parameter count)
                                    | *Parameter Response*
                                    
For each parameter in parameter count the parameter response is as follows:

* Integer (is nullable)
* Integer (scale)
* Integer (precision)
* Integer (type)

### GetTriggers (57)

_Request_                  | _Response_ 
-------------------------- | --------------------
String (catalog pattern)   | ResultSet (matched triggers)
String (schema pattern)    | 
String (table pattern)     | 
String (trigger pattern)   |

### GetTypeInfo (45)

_Request_     | _Response_ 
------------- | --------------------
None          | ResultSet (database metadata type info)

### GetPrimaryKeys (40)

_Request_                  | _Response_ 
-------------------------- | --------------------
String (catalog pattern)   | ResultSet (matching primary keys)
String (schema pattern)    | 
String (table pattern)     |

### GetMoreResults (46)

_Request_                  | _Response_ 
-------------------------- | --------------------
Integer (statement handle) | Integer (more results flag)

If the more results flag is set to 1 then there are more results to get.

### GetTables (36)

_Request_                  | _Response_ 
-------------------------- | --------------------
String (catalog pattern)   | ResultSet (matching tables)
String (schema pattern)    |
String (table pattern)     |
Integer (type count)       |
*Type Request*             |

For each type in type count the type request is as follows:

* String (matching type)

### GetTableTypes (44)

_Request_ | _Response_ 
--------- | --------------------
None      | ResultSet (metadata table types)

### GetResultSet (13)

_Request_                 | _Response_ 
------------------------- | ------------------------------------------
Integer (statment handle) | ResultSet (statement's current result set)

### GetSchemas (35)

_Request_ | _Response_ 
--------- | --------------------
None      | ResultSet (database metadata schemas)

### GetTransactionIsolation (63)

_Request_ | _Response_ 
--------- | --------------------
None      | Integer (transaction isolation level)

### GetUpdateCount (47)

_Request_                  | _Response_ 
-------------------------- | --------------------
Integer (statement handle) | Integer (update count)

### IsReadOnly (61)

_Request_ | _Response_ 
--------- | --------------------
None      | Integer (read-only flag)

If the read-only flag is set to 1 then it is read-only.

### Next (27)

_Request_                   | _Response_ 
--------------------------- | --------------------
Integer (result set handle) | Integer (next flag)
                            | *Field Response*
                            
If the next flag is set to 1 then there are still more rows and the message size isn't at max.  
If the next flag is set to 0 then the last row was encoded.
                            
For each column in column count the field response is as follows:

* TYPE (field value)

> NOTE: If this is a cursor, then the while loop executes at most once.


### OpenDatabase (3)

_Request_                      | _Response_ 
------------------------------ | --------------------
Integer (protocol version)     | Integer (version)
String (database name)         | String (server srp public key)
Integer (parameter count)      | String (server srp salt)
*Parameter Request*            | 
Integer (txn id)               |
String (client srp public key) |

For each parameter in parameter count the parameter request is as follows:

* String (param name)
* String (param value)

### Ping (48)

_Request_ | _Response_ 
--------- | --------------------
None      | None

### PrepareStatement (9)

_Request_          | _Response_ 
------------------ | --------------------
String (statement) | Integer (prepared statement handle)
                   | Integer (parameter count)

### PrepareStatementKeys (88)

_Request_                    | _Response_ 
---------------------------- | --------------------
Integer (generate keys flag) | Integer (prepared statement handle)
String (statement)           | Integer (parameter count)

If the generate keys flag is set to 0 then keys will not be generated.  

### PrepareStatementKeysIds (90)

_Request_               | _Response_ 
----------------------- | --------------------
Integer (key ids count) | Integer (prepared statement handle)
*Key IDs Request*       | Integer (parameter count)
String (statement)      | 

For each key id in key id count the key ids request is as follows:

* Integer (key id)

### PrepareStatementKeyNames (89)

_Request_                 | _Response_ 
------------------------- | --------------------
Integer (key names count) | Integer (prepared statement handle)
*Key Names Request*       | Integer (parameter count)

For each key name in key names count the key names request is as follows:

* String (key name)

### ReleaseSavePoint (98)

_Request_              | _Response_ 
---------------------- | --------------------
Integer (savepoint id) | None

### RollbackToSavePoint (99)

_Request_              | _Response_ 
---------------------- | --------------------
Integer (savepoint id) | None

### RollbackTransaction (8)

_Request_  | _Response_ 
---------- | --------------------
None       | None

### SetAutoCommit (60)

_Request_                  | _Response_ 
-------------------------- | --------------------
Integer (auto-commit flag) | None

If the auto-commit flag is set to non-zero then auto-commit will be turned on. 

### SetCursorName (21)

_Request_                  | _Response_ 
-------------------------- | --------------------
Integer (statement handle) | String (cursor name)
String (cursor name)       |

### SetReadOnly (62)

_Request_                  | _Response_ 
-------------------------- | --------------------
Integer (read-only flag)   | None

If the read-only flag is set to non-zero then read-only will be turned on. 

### SetSavePoint (97)

_Request_  | _Response_ 
---------- | --------------------
None       | Integer (savepoint id)

### SetTransactionIsolation (64)

_Request_                             | _Response_ 
------------------------------------- | --------------------
Integer (transaction isolation level) | None

### SetTraceFlags (72)

_Request_      | _Response_ 
-------------- | --------------------
Integer (mask) | Integer (result)

### StatementAnalyze (71)

_Request_                  | _Response_ 
-------------------------- | --------------------
Integer (statement handle) | String (result)
Integer (mask)             |

### SupportTransactionIsolation (100)

_Request_                             | _Response_ 
------------------------------------- | --------------------
Integer (transaction isolation level) | Integer (supported level flag)

##### Supported level flag
_Value_   | _Meaning_
--------- | ------------------------
1         | The transaction isolation level is supported.  
0         | The transaction isolation level is unsupported.

