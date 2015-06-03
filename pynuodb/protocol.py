"""Contains the constants for sending the protocol to the server."""

AUTH_TEST_STR = 'Success!'

# Data Types Encoding Rules
NULL                            = 1
TRUE                            = 2
FALSE                           = 3
UUID                            = 200
SCALEDCOUNT1                    = 199
SCALEDCOUNT2                    = 225
INTMINUS10                      = 10
INTMINUS1                       = 19
INT0                            = 20
INT31                           = 51
INTLEN1                         = 52
INTLEN8                         = 59
SCALEDLEN0                      = 60
SCALEDLEN8                      = 68
DOUBLELEN0                      = 77
DOUBLELEN8                      = 85
UTF8COUNT1                      = 69
UTF8COUNT4                      = 72
UTF8LEN0                        = 109
UTF8LEN39                       = 148
OPAQUECOUNT1                    = 73
OPAQUECOUNT4                    = 76
OPAQUELEN0                      = 149
OPAQUELEN39                     = 188
BLOBLEN0                        = 189
BLOBLEN4                        = 193
CLOBLEN0                        = 194
CLOBLEN4                        = 198
MILLISECLEN0                    = 86     # milliseconds since January 1, 1970
MILLISECLEN8                    = 94
NANOSECLEN0                     = 95     # nanoseconds since January 1, 1970
NANOSECLEN8                     = 103
TIMELEN0                        = 104    # milliseconds since midnight
TIMELEN4                        = 108
SCALEDTIMELEN1                  = 209
SCALEDTIMELEN8                  = 216
SCALEDTIMESTAMPLEN1             = 217
SCALEDTIMESTAMPLEN8             = 224
SCALEDDATELEN1                  = 201
SCALEDDATELEN8                  = 208


# Protocol Messages
FAILURE                         = 0
SUCCESS                         = 1
SHUTDOWN                        = 2
OPENDATABASE                    = 3
CREATEDATABASE                  = 4
CLOSE                           = 5
PREPARETRANSACTION              = 6
COMMITTRANSACTION               = 7
ROLLBACKTRANSACTION             = 8
PREPARE                         = 9
PREPAREDRL                      = 10
CREATE                          = 11
GENHTML                         = 12
GETRESULTSET                    = 13
SEARCH                          = 14
CLOSESTATMENT                   = 15
CLEARPARAMETERS                 = 16
GETPARAMETERCOUNT               = 17
EXECUTE                         = 18
EXECUTEQUERY                    = 19
EXECUTEUPDATE                   = 20
SETCURSORNAME                   = 21
EXECUTEPREPAREDSTATEMENT        = 22
EXECUTEPREPAREDQUERY            = 23
EXECUTEPREPAREDUPDATE           = 24
SENDPARAMETERS                  = 25
GETMETADATA                     = 26
NEXT                            = 27
CLOSERESULTSET                  = 28
GENHTML                         = 29
NEXTHIT                         = 30
FETCHRECORD                     = 31
CLOSERESULTLIST                 = 32
GET                             = 33 
GETCATALOGS                     = 34 
GETSCHEMAS                      = 35 
GETTABLES                       = 36 
GETTABLEPRIVILEGES              = 37 
GETCOLUMNS                      = 38 
GETCOLUMNSPRIVILEGES            = 39 
GETPRIMARYKEYS                  = 40 
GETIMPORTEDKEYS                 = 41 
GETEXPORTEDKEYS                 = 42 
GETINDEXINFO                    = 43 
GETTABLETYPES                   = 44 
GETTYPEINFO                     = 45 
GETMORERESULTS                  = 46 
GETUPDATECOUNT                  = 47
PING                            = 48 
HASROLE                         = 49 
GETOBJECTPRIVILEGES             = 50 
GETUSERROLES                    = 51 
GETROLES                        = 52 
GETUSERS                        = 53
OPENDATABASE2                   = 54 
CREATEDATABASE2                 = 55 
INITIATESERVICE                 = 56
GETTRIGGERS                     = 57 
VALIDATE                        = 58
GETAUTOCOMMIT                   = 59
SETAUTOCOMMIT                   = 60
ISREADONLY                      = 61
SETREADONLY                     = 62
GETTRANSACTIONISOLATION         = 63
SETTRANSACTIONISOLATION         = 64
GETSEQUENCEVALUE                = 65
ADDLOGLISTENER                  = 66 
DELETELOGLISTENER               = 67 
GETDATABASEPRODUCTNAME          = 68 
GETDATABASEPRODUCTVERSION       = 69 
ANALYZE                         = 70 
STATEMENTANALYZE                = 71 
SETTRACEFLAGS                   = 72 
SERVEROPERATION                 = 73 
SETATTRIBUTE                    = 74 
GETSEQUENCES                    = 75 
GETHOLDERPRIVILEGES             = 76 
ATTACHDEBUGGER                  = 77 
DEBUGREQUEST                    = 78 
GETSEQUENCEVALUE2               = 79 
GETCONNECTIONLIMIT              = 80 
SETCONNECTIONLIMIT              = 81 
DELETEBLOBDATA                  = 82 
EXECUTEBATCH                    = 83
EXECUTEBATCHPREPAREDSTATEMENT   = 84
GETPARAMETERMETADATA            = 85 
AUTHENTICATION                  = 86
GETGENERATEDKEYS                = 87 
PREPAREKEYS                     = 88 
PREPAREKEYNAMES                 = 89 
PREPAREKEYIDS                   = 90 
EXECUTEKEYS                     = 91 
EXECUTEKEYNAMES                 = 92 
EXECUTEKEYIDS                   = 93 
EXECUTEUPDATEKEYS               = 94 
EXECUTEUPDATEKEYNAMES           = 95 
EXECUTEUPDATEKEYIDS             = 96 
SETSAVEPOINT                    = 97 
RELEASESAVEPOINT                = 98 
ROLLBACKTOSAVEPOINT             = 99 
SUPPORTSTRANSACTIONISOLATION    = 100 
GETCATALOG                      = 101 
GETCURRENTSCHEMA                = 102 
PREPARECALL                     = 103
EXECUTECALLABLESTATEMENT        = 104 
SETQUERYTIMEOUT                 = 105 
GETPROCEDURES                   = 106 
GETPROCEDURECOLUMNS             = 107 
GETSUPERTABLES                  = 108
GETSUPERTYPES                   = 109 
GETFUNCTIONS                    = 110 
GETFUNCTIONCOLUMNS              = 111 
GETTABLEPRIVILEGES              = 112 
GETCOLUMNPRIVILEGES             = 113 
GETCROSSREFERENCE               = 114 
ALLPROCEDURESARECALLABLE        = 115 
ALLTABLESARESELECTABLE          = 116 
GETATTRIBUTES                   = 117 
GETUDTS                         = 118
GETVERSIONCOLUMNS               = 119 
GETLOBCHUNK                     = 120
GETLASTSTATEMENTTIMEMICROS      = 121




# Error code values
SYNTAX_ERROR                    = -1
FEATURE_NOT_YET_IMPLEMENTED     = -2
BUG_CHECK                       = -3
COMPILE_ERROR                   = -4
RUNTIME_ERROR                   = -5
OCS_ERROR                       = -6
NETWORK_ERROR                   = -7
CONVERSION_ERROR                = -8
TRUNCATION_ERROR                = -9
CONNECTION_ERROR                = -10
DDL_ERROR                       = -11
APPLICATION_ERROR               = -12
SECURITY_ERROR                  = -13
DATABASE_CORRUPTION             = -14
VERSION_ERROR                   = -15
LICENSE_ERROR                   = -16
INTERNAL_ERROR                  = -17
DEBUG_ERROR                     = -18
LOST_BLOB                       = -19
INCONSISTENT_BLOB               = -20
DELETED_BLOB                    = -21
LOG_ERROR                       = -22
DATABASE_DAMAGED                = -23
UPDATE_CONFLICT                 = -24
NO_SUCH_TABLE                   = -25
INDEX_OVERFLOW                  = -26
UNIQUE_DUPLICATE                = -27
UNCOMMITTED_UPDATES             = -28
DEADLOCK                        = -29
OUT_OF_MEMORY_ERROR             = -30
OUT_OF_RECORD_MEMORY_ERROR      = -31
LOCK_TIMEOUT                    = -32
PLATFORM_ERROR                  = -36
NO_SCHEMA                       = -37
CONFIGURATION_ERROR             = -38
READ_ONLY_ERROR                 = -39
NO_GENERATED_KEYS               = -40
THROWN_EXCEPTION                = -41
INVALID_TRANSACTION_ISOLATION   = -42
UNSUPPORTED_TRANSACTION_ISOLATION = -43
INVALID_UTF8                    = -44
CONSTRAINT_ERROR                = -45
UPDATE_ERROR                    = -46
I18N_ERROR                      = -47
OPERATION_KILLED                = -48
INVALID_STATEMENT               = -49
IS_SHUTDOWN                     = -50
IN_QUOTED_STRING                = -51
BATCH_UPDATE_ERROR              = -52
JAVA_ERROR                      = -53
INVALID_FIELD                   = -54
INVALID_INDEX_NULL              = -55
INVALID_OPERATION               = -56

DATA_ERRORS = {COMPILE_ERROR,
               RUNTIME_ERROR,
               CONVERSION_ERROR,
               TRUNCATION_ERROR,
               VERSION_ERROR,
               INVALID_UTF8,
               I18N_ERROR}

OPERATIONAL_ERRORS = {NETWORK_ERROR,
                      DDL_ERROR,
                      PLATFORM_ERROR,
                      BATCH_UPDATE_ERROR,
                      OPERATION_KILLED,
                      INVALID_STATEMENT,
                      INVALID_OPERATION}

INTERNAL_ERRORS = {DATABASE_CORRUPTION,
                   INTERNAL_ERROR,
                   UPDATE_CONFLICT,
                   DEADLOCK,
                   IS_SHUTDOWN}

PROGRAMMING_ERRORS = {SYNTAX_ERROR,
                      CONNECTION_ERROR,
                      APPLICATION_ERROR,
                      SECURITY_ERROR,
                      NO_SUCH_TABLE,
                      NO_SCHEMA,
                      CONFIGURATION_ERROR,
                      READ_ONLY_ERROR,
                      IN_QUOTED_STRING}

NOT_SUPPORTED_ERRORS = {FEATURE_NOT_YET_IMPLEMENTED,
                        UNSUPPORTED_TRANSACTION_ISOLATION}


stringifyError = {
                      SYNTAX_ERROR                    : 'SYNTAX_ERROR',
                      FEATURE_NOT_YET_IMPLEMENTED     : 'FEATURE_NOT_YET_IMPLEMENTED',
                      BUG_CHECK                       : 'BUG_CHECK',
                      COMPILE_ERROR                   : 'COMPILE_ERROR',
                      RUNTIME_ERROR                   : 'RUNTIME_ERROR',
                      OCS_ERROR                       : 'OCS_ERROR',
                      NETWORK_ERROR                   : 'NETWORK_ERROR',
                      CONVERSION_ERROR                : 'CONVERSION_ERROR',
                      TRUNCATION_ERROR                : 'TRUNCATION_ERROR',
                      CONNECTION_ERROR                : 'CONNECTION_ERROR',
                      DDL_ERROR                       : 'DDL_ERROR',
                      APPLICATION_ERROR               : 'APPLICATION_ERROR',
                      SECURITY_ERROR                  : 'SECURITY_ERROR',
                      DATABASE_CORRUPTION             : 'DATABASE_CORRUPTION',
                      VERSION_ERROR                   : 'VERSION_ERROR',
                      LICENSE_ERROR                   : 'LICENSE_ERROR',
                      INTERNAL_ERROR                  : 'INTERNAL_ERROR',
                      DEBUG_ERROR                     : 'DEBUG_ERROR',
                      LOST_BLOB                       : 'LOST_BLOB',
                      INCONSISTENT_BLOB               : 'INCONSISTENT_BLOB',
                      DELETED_BLOB                    : 'DELETED_BLOB',
                      LOG_ERROR                       : 'LOG_ERROR',
                      DATABASE_DAMAGED                : 'DATABASE_DAMAGED',
                      UPDATE_CONFLICT                 : 'UPDATE_CONFLICT',
                      NO_SUCH_TABLE                   : 'NO_SUCH_TABLE',
                      INDEX_OVERFLOW                  : 'INDEX_OVERFLOW',
                      UNIQUE_DUPLICATE                : 'UNIQUE_DUPLICATE',
                      UNCOMMITTED_UPDATES             : 'UNCOMMITTED_UPDATES',
                      DEADLOCK                        : 'DEADLOCK',
                      OUT_OF_MEMORY_ERROR             : 'OUT_OF_MEMORY_ERROR',
                      OUT_OF_RECORD_MEMORY_ERROR      : 'OUT_OF_RECORD_MEMORY_ERROR',
                      LOCK_TIMEOUT                    : 'LOCK_TIMEOUT',
                      PLATFORM_ERROR                  : 'PLATFORM_ERROR',
                      NO_SCHEMA                       : 'NO_SCHEMA',
                      CONFIGURATION_ERROR             : 'CONFIGURATION_ERROR',
                      READ_ONLY_ERROR                 : 'READ_ONLY_ERROR',
                      NO_GENERATED_KEYS               : 'NO_GENERATED_KEYS',
                      THROWN_EXCEPTION                : 'THROWN_EXCEPTION',
                      INVALID_TRANSACTION_ISOLATION   : 'INVALID_TRANSACTION_ISOLATION',
                      UNSUPPORTED_TRANSACTION_ISOLATION : 'UNSUPPORTED_TRANSACTION_ISOLATION',
                      INVALID_UTF8                    : 'INVALID_UTF8',
                      CONSTRAINT_ERROR                : 'CONSTRAINT_ERROR',
                      UPDATE_ERROR                    : 'UPDATE_ERROR',
                      I18N_ERROR                      : 'I18N_ERROR',
                      OPERATION_KILLED                : 'OPERATION_KILLED',
                      INVALID_STATEMENT               : 'INVALID_STATEMENT',
                      IS_SHUTDOWN                     : 'IS_SHUTDOWN',
                      IN_QUOTED_STRING                : 'IN_QUOTED_STRING',
                      BATCH_UPDATE_ERROR              : 'BATCH_UPDATE_ERROR',
                      JAVA_ERROR                      : 'JAVA_ERROR',
                      INVALID_FIELD                   : 'INVALID_FIELD',
                      INVALID_INDEX_NULL              : 'INVALID_INDEX_NULL',
                      INVALID_OPERATION               : 'INVALID_OPERATION'
                 }

def lookup_code(error_code):
    return stringifyError.get(error_code, '[UNKNOWN ERROR CODE]')

#
# NuoDB Client-Server Protocol Version #'s
#
PROTOCOL_VERSION1          = 1
PROTOCOL_VERSION2          = 2   # 3/27/2011    Passing SQLState on exceptions; piggybacking generated
                                 #              key result sets.
PROTOCOL_VERSION3          = 3   # 2/14/2012    Passing txn, node id and commit sequence
PROTOCOL_VERSION4          = 4   # 2/29/2012    Added GetCurrentSchema
PROTOCOL_VERSION5          = 5   # 3/12/2012    Server returns DB UUID
PROTOCOL_VERSION6          = 6   # 3/22/2012    Server encodes error message for each statement in batch
PROTOCOL_VERSION7          = 7   # 5/11/2012    Fix issues with generated key result set and last commit info
PROTOCOL_VERSION8          = 8   # 8/10/2012    Optimize result set close to be async and support to skip column header
PROTOCOL_VERSION9          = 9   # 8/30/2012    New Date/Time/Timestamp encodings
PROTOCOL_VERSION10         = 10  # 11/13/2012   Normalized Dates - NuoDB GA 1.0
PROTOCOL_VERSION11         = 11  # 02/01/2013   New BigInt - NuoDB GA 1.0.2
PROTOCOL_VERSION12         = 12  # 06/28/2013   Added setQueryTimeout/PrepareCall/ExecuteCall
PROTOCOL_VERSION13         = 13  # 01/24/2013   Added some JDBC methods
PROTOCOL_VERSION14         = 14  # 02/18/2014   Changed strings in Types/TypeUtil.cpp - affected Python driver
PROTOCOL_VERSION15         = 15  # 04/22/2014   openDatabase returns server-side connection id in response
PROTOCOL_VERSION16         = 16  # 07/01/2014
PROTOCOL_VERSION17         = 17  # 11/03/2014 Support for client sending last commit info
PROTOCOL_VERSION18         = 18  # 02/19/2015 JDBC metadata updates
PROTOCOL_VERSION19         = 19  # 05/01/2015 Server timing of statements
#
# The current protocol version of THIS driver.  The server will negotiate the lowest compatible version.
#
CURRENT_PROTOCOL_VERSION   = PROTOCOL_VERSION14
