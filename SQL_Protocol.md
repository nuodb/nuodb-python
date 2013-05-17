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
202          | 32-byte UUID      
201          | Fixed point integer with scale  
227          | Fixed point integer with scale      

#### Integer Types

Field Header | Field Type & Format
------------ | --------------------
10-19        | The Integer values `-10` to `-1` (respectively)                  
20-51        | The Integer values `0` to `31` (respectively)      
52-59        | A signed integer value encoded using the next 1 to 8 bytes (respectively)    

E.g., a field that starts with the identifier 21 represents the Integer value 1, and has no further payload. A field that starts with 52 is followed by a single byte that encodes an 8-bit signed integer value.

#### Scaled Integer Types

Field Header | Field Type & Format
------------ | --------------------
60-68        | A scaled integer value of length 0 to length 8 bytes (respectively). The format is one byte giving the scale followed by 0 to 8 bytes with the scaled integer value.

#### Double Precision Types

Field Header | Field Type & Format
------------ | --------------------
77-85        | IEEE double-precision floating point value of length 0 to length 8 bytes (respectively)

#### String (UTF8) Types

Field Header | Field Type & Format
------------ | --------------------
69-72        | A string where the first 1 to 4 bytes (respectively) encode an integer that is the number of bytes that follow as the string value                 
109-148      | A string of length 0 to 39 bytes (respectively)     
149          | A string of length 39 bytes (constant for the longest known-length string)

All strings are UTF-8 encoded lengths of bytes, with a header that specifies the number of bytes in the string. For instance, a field starting with the identifier 70 will be followed by two bytes that encode a 16-bit integer value. That value is the length of string. A field starting with 112 will be followed by a UTF-8 string three bytes long.

#### Opaque Types

Field Header | Field Type & Format
------------ | --------------------
73-76        | An opaque value where the first 1 to 4 bytes (respectively) encode an integer that is the number of bytes that follow as the value                
150-189      | An opaque value of length 0 to 39 bytes (respectively)   
190          | An opaque value of length 39 bytes (constant for the longest known-length opaque value)

#### BLOB/CLOB Types

Field Header | Field Type & Format
------------ | --------------------
191-195      | A BLOB (Binary Large OBject) value where the first 0 to 4 bytes (respectively) encode an integer that is the number of bytes that follow as the binary value                
196 - 200    | A CLOB (Character Large OBject) value where the first 0 to 4 bytes (respectively) encode an integer that is the number of bytes that follow as the character string 

#### Time Types

Field Header | Field Type & Format
------------ | --------------------
86-94        | Milliseconds since the epoch of length 0 to length 8 bytes (respectively)                
95-103       | Nanoseconds since the epoch of length 0 to length 8 bytes (respectively) 
104-108      | Milliseconds since midnight of length 0 to length 4 bytes (respectively)

#### Scaled Time Types

Field Header | Field Type & Format
------------ | --------------------
211-218      | A scaled time value of length 0 to length 8 bytes (respectively). The format is one byte giving the scale followed by 0 to 8 bytes with the scaled time value.               
219-226      | A scaled timestamp value of length 0 to length 8 bytes (respectively). The format is one byte giving the scale followed by 0 to 8 bytes with the scaled timestamp value.

#### Scaled Date Types

Field Header | Field Type & Format
------------ | --------------------
203-210      | A scaled date value of length 0 to length 8 bytes (respectively). The format is one byte giving the scale followed by 0 to 8 bytes with the scaled date value.

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
--------- | ----------
None      | 64-bit Integer (transaction id), Integer (node id), 64-bit Integer (commit sequence) 

### CreateStatement (11)

_Request_                    | _Response_ 
---------------------------  | ----------
None                         | Integer (statement handle) 

If auto-commit is true on this connection, then creating a new statement causes any existing transaction to commit.

### Execute (18)

_Request_                                       | _Response_ 
----------------------------------------------  | ----------
Integer (statement handle), String (statement)  | EXECUTE RESPONSE (1 if execution is finished, 0 otherwise) 

Blocks pending commit

### ExecuteBatchStatement (83)

Blocks pending commit

In the case of failed execution, this will return the specific error message:

Integer (EXECUTE_FAILED = -3), Integer (SQL error code), String (error message)

### ExecuteBatchPreparedStatement (84)
### ExecuteKeys (91)
### ExecuteKeyIds (93)
### ExecuteKeyNames (92)
### ExecutePreparedStatement (22)
### ExecutePreparedQuery (23)
### ExecutePreparedUpdate (24)
### ExecuteQuery (19)
### ExecuteUpdate (20)
### ExecuteUpdateKeys (94)
### ExecuteUpdateKeyIds (96)
### ExecuteUpdateKeyNames (95)
### GetAutoCommit (59)
### GetCatalog (101)
### GetCatalogs (34)
### GetColumns (102)
### GetCurrentSchema (102)
### GetDatabaseMetaData (33)
### GetGenerateKeys (87)
### GetImportedKeys (41)
### GetIndexInfo (43)
### GetMetaData (26)
### GetParameterMetaData (85)
### GetTriggers (57)
### GetTypeInfo (45)
### GetPrimaryKeys (40)
### GetMoreResults (46)
### GetTables (36)
### GetTableTypes (44)
### GetResultSet (13)
### GetSchemas (35)
### GetTransactionIsolation (63)
### GetUpdateCount (47)
### IsReadOnly (61)
### Next (27)
### OpenDatabase (3)
### Ping (48)
### PrepareStatement (9)
### PrepareStatementKeys (88)
### PrepareStatementKeysIds (90)
### PrepareStatementKeyNames (89)
### ReleaseSavePoint (98)
### RollbackToSavePoint (99)
### RollbackTransaction (8)
### SetAutoCommit (60)
### SetCursorName (21)
### SetReadOnly (62)
### SetSavePoint (97)
### SetTransactionIsolation (64)
### SetTraceFlags (72)
### StatementAnalyze (71)
### SupportTransactionIsolcation (100)

