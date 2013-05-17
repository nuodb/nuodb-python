# NuoDB SQL Protocol

## Overview
This document describes the protocol between SQL clients and Transaction Engines in NuoDB. With the exception of an initial handshake, all messages are tightly encoded in a binary representation and encrypted.

Note that the protocol is designed to handle revisions and be compatible across versions. Even before the first public release of the product several revisions were made. This document covers revisions from Protocol Version 10 to Protocol Version 11.

When the protocol is updated this document should also be updated to reflect those changes.

The following NuoDB releases included changes to the SQL Protocol Version:
| NuoDB Version       | SQL Protocol Version |
| ------------------- | -------------------- |
| 1.0                 | 10                   |
| 1.0.2               | 10                   |

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


## Protocol Messages