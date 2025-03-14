"""Constants for the message protocol with the NuoDB database.

(C) Copyright 2013-2023 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

# pylint: disable=bad-whitespace

# Data Types Encoding Rules
NULL                              = 1
TRUE                              = 2
FALSE                             = 3
INTMINUS10                        = 10
INTMINUS1                         = 19
INT0                              = 20
INT31                             = 51
INTLEN0                           = 51
INTLEN1                           = 52
INTLEN8                           = 59
SCALEDLEN0                        = 60
SCALEDLEN8                        = 68
UTF8COUNT0                        = 68
UTF8COUNT1                        = 69
UTF8COUNT4                        = 72
OPAQUECOUNT0                      = 72
OPAQUECOUNT1                      = 73
OPAQUECOUNT4                      = 76
DOUBLELEN0                        = 77
DOUBLELEN8                        = 85
MILLISECLEN0                      = 86     # milliseconds since January 1, 1970
MILLISECLEN8                      = 94
NANOSECLEN0                       = 95     # nanoseconds since January 1, 1970
NANOSECLEN8                       = 103
TIMELEN0                          = 104    # milliseconds since midnight
TIMELEN4                          = 108
UTF8LEN0                          = 109
UTF8LEN39                         = 148
OPAQUELEN0                        = 149
OPAQUELEN39                       = 188
BLOBLEN0                          = 189
BLOBLEN4                          = 193
CLOBLEN0                          = 194
CLOBLEN4                          = 198
SCALEDCOUNT1                      = 199
UUID                              = 200
SCALEDDATELEN0                    = 200
SCALEDDATELEN1                    = 201
SCALEDDATELEN8                    = 208
SCALEDTIMELEN0                    = 208
SCALEDTIMELEN1                    = 209
SCALEDTIMELEN8                    = 216
SCALEDTIMESTAMPLEN0               = 216
SCALEDTIMESTAMPLEN1               = 217
SCALEDTIMESTAMPLEN8               = 224
SCALEDCOUNT2                      = 225

# Protocol Messages
FAILURE                           = 0
OPENDATABASE                      = 3
CLOSE                             = 5
PREPARETRANSACTION                = 6
COMMITTRANSACTION                 = 7
ROLLBACKTRANSACTION               = 8
PREPARE                           = 9
CREATE                            = 11
GETRESULTSET                      = 13
CLOSESTATEMENT                    = 15
EXECUTE                           = 18
EXECUTEQUERY                      = 19
EXECUTEUPDATE                     = 20
SETCURSORNAME                     = 21
EXECUTEPREPAREDSTATEMENT          = 22
EXECUTEPREPAREDQUERY              = 23
EXECUTEPREPAREDUPDATE             = 24
GETMETADATA                       = 26
NEXT                              = 27
CLOSERESULTSET                    = 28
GET                               = 33
GETCATALOGS                       = 34
GETSCHEMAS                        = 35
GETTABLES                         = 36
GETCOLUMNS                        = 38
GETPRIMARYKEYS                    = 40
GETIMPORTEDKEYS                   = 41
GETEXPORTEDKEYS                   = 42
GETINDEXINFO                      = 43
GETTABLETYPES                     = 44
GETTYPEINFO                       = 45
GETMORERESULTS                    = 46
GETUPDATECOUNT                    = 47
PING                              = 48
GETTRIGGERS                       = 57
GETAUTOCOMMIT                     = 59
SETAUTOCOMMIT                     = 60
ISREADONLY                        = 61
SETREADONLY                       = 62
GETTRANSACTIONISOLATION           = 63
SETTRANSACTIONISOLATION           = 64
GETSEQUENCEVALUE                  = 65
ANALYZE                           = 70
STATEMENTANALYZE                  = 71
SETTRACEFLAGS                     = 72
EXECUTEBATCH                      = 83
EXECUTEBATCHPREPAREDSTATEMENT     = 84
GETPARAMETERMETADATA              = 85
AUTHENTICATION                    = 86
GETGENERATEDKEYS                  = 87
PREPAREKEYS                       = 88
PREPAREKEYNAMES                   = 89
PREPAREKEYIDS                     = 90
EXECUTEKEYS                       = 91
EXECUTEKEYNAMES                   = 92
EXECUTEKEYIDS                     = 93
EXECUTEUPDATEKEYS                 = 94
EXECUTEUPDATEKEYNAMES             = 95
EXECUTEUPDATEKEYIDS               = 96
SETSAVEPOINT                      = 97
RELEASESAVEPOINT                  = 98
ROLLBACKTOSAVEPOINT               = 99
SUPPORTSTRANSACTIONISOLATION      = 100
GETCATALOG                        = 101
GETCURRENTSCHEMA                  = 102
PREPARECALL                       = 103
EXECUTECALLABLESTATEMENT          = 104
SETQUERYTIMEOUT                   = 105
GETPROCEDURES                     = 106
GETPROCEDURECOLUMNS               = 107
GETSUPERTABLES                    = 108
GETSUPERTYPES                     = 109
GETFUNCTIONS                      = 110
GETFUNCTIONCOLUMNS                = 111
GETTABLEPRIVILEGES                = 112
GETCOLUMNPRIVILEGES               = 113
GETCROSSREFERENCE                 = 114
ALLPROCEDURESARECALLABLE          = 115
ALLTABLESARESELECTABLE            = 116
GETATTRIBUTES                     = 117
GETUDTS                           = 118
GETVERSIONCOLUMNS                 = 119
GETLOBCHUNK                       = 120
GETLASTSTATEMENTTIMEMICROS        = 121
AUTHORIZETYPESREQUEST             = 122
SETRESULTSETFETCHSIZE             = 123
SETSTATEMENTFETCHSIZE             = 124
RECOVERTRANSACTION                = 125
SQLTEST                           = 126

# Error code values
SYNTAX_ERROR                      = -1
FEATURE_NOT_YET_IMPLEMENTED       = -2
BUG_CHECK                         = -3
COMPILE_ERROR                     = -4
RUNTIME_ERROR                     = -5
OCS_ERROR                         = -6
NETWORK_ERROR                     = -7
CONVERSION_ERROR                  = -8
TRUNCATION_ERROR                  = -9
CONNECTION_ERROR                  = -10
DDL_ERROR                         = -11
APPLICATION_ERROR                 = -12
SECURITY_ERROR                    = -13
DATABASE_CORRUPTION               = -14
VERSION_ERROR                     = -15
LICENSE_ERROR                     = -16
INTERNAL_ERROR                    = -17
DEBUG_ERROR                       = -18
LOST_BLOB                         = -19
INCONSISTENT_BLOB                 = -20
DELETED_BLOB                      = -21
LOG_ERROR                         = -22
DATABASE_DAMAGED                  = -23
UPDATE_CONFLICT                   = -24
NO_SUCH_TABLE                     = -25
INDEX_OVERFLOW                    = -26
UNIQUE_DUPLICATE                  = -27
UNCOMMITTED_UPDATES               = -28
DEADLOCK                          = -29
OUT_OF_MEMORY_ERROR               = -30
OUT_OF_RECORD_MEMORY_ERROR        = -31
LOCK_TIMEOUT                      = -32
PLATFORM_ERROR                    = -36
NO_SCHEMA                         = -37
CONFIGURATION_ERROR               = -38
READ_ONLY_ERROR                   = -39
NO_GENERATED_KEYS                 = -40
THROWN_EXCEPTION                  = -41
INVALID_TRANSACTION_ISOLATION     = -42
UNSUPPORTED_TRANSACTION_ISOLATION = -43
INVALID_UTF8                      = -44
CONSTRAINT_ERROR                  = -45
UPDATE_ERROR                      = -46
I18N_ERROR                        = -47
OPERATION_KILLED                  = -48
INVALID_STATEMENT                 = -49
IS_SHUTDOWN                       = -50
IN_QUOTED_STRING                  = -51
BATCH_UPDATE_ERROR                = -52
JAVA_ERROR                        = -53
INVALID_FIELD                     = -54
INVALID_INDEX_NULL                = -55
INVALID_OPERATION                 = -56
INVALID_STATISTICS                = -57
INVALID_GENERATOR                 = -58
OPERATION_TIMEOUT                 = -59
NO_SUCH_INDEX                     = -60
NO_SUCH_SEQUENCE                  = -61
XAER_PROTO                        = -62
UNKNOWN_ERROR                     = -63
TRANSACTIONAL_LOCK_ERROR          = -64
TRANSACTION_UNKNOWN_STATE         = -65
LOCK_NOT_GRANTED                  = -66


DATA_ERRORS = {COMPILE_ERROR,
               CONSTRAINT_ERROR,
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

INTEGRITY_ERRORS = {UNIQUE_DUPLICATE}

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
    SYNTAX_ERROR: 'SYNTAX_ERROR',
    FEATURE_NOT_YET_IMPLEMENTED: 'FEATURE_NOT_YET_IMPLEMENTED',
    BUG_CHECK: 'BUG_CHECK',
    COMPILE_ERROR: 'COMPILE_ERROR',
    RUNTIME_ERROR: 'RUNTIME_ERROR',
    OCS_ERROR: 'OCS_ERROR',
    NETWORK_ERROR: 'NETWORK_ERROR',
    CONVERSION_ERROR: 'CONVERSION_ERROR',
    TRUNCATION_ERROR: 'TRUNCATION_ERROR',
    CONNECTION_ERROR: 'CONNECTION_ERROR',
    DDL_ERROR: 'DDL_ERROR',
    APPLICATION_ERROR: 'APPLICATION_ERROR',
    SECURITY_ERROR: 'SECURITY_ERROR',
    DATABASE_CORRUPTION: 'DATABASE_CORRUPTION',
    VERSION_ERROR: 'VERSION_ERROR',
    LICENSE_ERROR: 'LICENSE_ERROR',
    INTERNAL_ERROR: 'INTERNAL_ERROR',
    DEBUG_ERROR: 'DEBUG_ERROR',
    LOST_BLOB: 'LOST_BLOB',
    INCONSISTENT_BLOB: 'INCONSISTENT_BLOB',
    DELETED_BLOB: 'DELETED_BLOB',
    LOG_ERROR: 'LOG_ERROR',
    DATABASE_DAMAGED: 'DATABASE_DAMAGED',
    UPDATE_CONFLICT: 'UPDATE_CONFLICT',
    NO_SUCH_TABLE: 'NO_SUCH_TABLE',
    INDEX_OVERFLOW: 'INDEX_OVERFLOW',
    UNIQUE_DUPLICATE: 'UNIQUE_DUPLICATE',
    UNCOMMITTED_UPDATES: 'UNCOMMITTED_UPDATES',
    DEADLOCK: 'DEADLOCK',
    OUT_OF_MEMORY_ERROR: 'OUT_OF_MEMORY_ERROR',
    OUT_OF_RECORD_MEMORY_ERROR: 'OUT_OF_RECORD_MEMORY_ERROR',
    LOCK_TIMEOUT: 'LOCK_TIMEOUT',
    PLATFORM_ERROR: 'PLATFORM_ERROR',
    NO_SCHEMA: 'NO_SCHEMA',
    CONFIGURATION_ERROR: 'CONFIGURATION_ERROR',
    READ_ONLY_ERROR: 'READ_ONLY_ERROR',
    NO_GENERATED_KEYS: 'NO_GENERATED_KEYS',
    THROWN_EXCEPTION: 'THROWN_EXCEPTION',
    INVALID_TRANSACTION_ISOLATION: 'INVALID_TRANSACTION_ISOLATION',
    UNSUPPORTED_TRANSACTION_ISOLATION: 'UNSUPPORTED_TRANSACTION_ISOLATION',
    INVALID_UTF8: 'INVALID_UTF8',
    CONSTRAINT_ERROR: 'CONSTRAINT_ERROR',
    UPDATE_ERROR: 'UPDATE_ERROR',
    I18N_ERROR: 'I18N_ERROR',
    OPERATION_KILLED: 'OPERATION_KILLED',
    INVALID_STATEMENT: 'INVALID_STATEMENT',
    IS_SHUTDOWN: 'IS_SHUTDOWN',
    IN_QUOTED_STRING: 'IN_QUOTED_STRING',
    BATCH_UPDATE_ERROR: 'BATCH_UPDATE_ERROR',
    JAVA_ERROR: 'JAVA_ERROR',
    INVALID_FIELD: 'INVALID_FIELD',
    INVALID_INDEX_NULL: 'INVALID_INDEX_NULL',
    INVALID_OPERATION: 'INVALID_OPERATION',
    INVALID_STATISTICS: 'INVALID_STATISTICS',
    INVALID_GENERATOR: 'INVALID_GENERATOR',
    OPERATION_TIMEOUT: 'OPERATION_TIMEOUT',
    NO_SUCH_INDEX: 'NO_SUCH_INDEX',
    NO_SUCH_SEQUENCE: 'NO_SUCH_SEQUENCE',
    XAER_PROTO: 'XAER_PROTO',
    UNKNOWN_ERROR: 'UNKNOWN_ERROR',
    TRANSACTIONAL_LOCK_ERROR: 'TRANSACTIONAL_LOCK_ERROR',
    TRANSACTION_UNKNOWN_STATE: 'TRANSACTION_UNKNOWN_STATE',
    LOCK_NOT_GRANTED: 'LOCK_NOT_GRANTED'
}


def lookup_code(error_code):
    # type: (int) -> str
    """Return a string-ified version of an error code."""
    return stringifyError.get(error_code, '[UNKNOWN ERROR CODE]')


#
# NuoDB Client-Server Features
#

NORMALIZED_DATES                          = 10
ARBITRARY_DECIMAL                         = 11
PREPARE_CALL                              = 12
SET_QUERY_TIMEOUT                         = 12
MORE_JDBC                                 = 13
STRING_CHANGE                             = 14
SEND_CONNID_TO_CLIENT                     = 15
USE_ACTUAL_DECLARED_TYPE                  = 15
GET_TABLE_PRIVILEGES                      = 16
GET_CROSS_REFERENCE                       = 16
SEND_EFFECTIVE_PLATFORM_VERSION_TO_CLIENT = 16
LAST_COMMIT_INFO                          = 17
JDBC_METADATA_UPDATES                     = 18
LOB_STREAMING                             = 18
SERVER_TIMING                             = 19
STORED_PROC_ARRAY_ARGS                    = 19
OPERATION_TIMEOUT_ERROR                   = 19
SET_FETCH_SIZE                            = 19
XA_TRANSACTIONS                           = 19
BIGINT_ENCODE_VER3                        = 20 #TBD
SEND_PREPARE_STMT_RESULT_SET_METADATA_TO_CLIENT = 21 #TBD
DDL_NOT_AUTOCOMMITTED                     = 22 #TBD
MULTI_CIPHER                              = 23 #TBD
CURSOR_HOLDABILITY                        = 24 #TBD
TIMESTAMP_WITHOUT_TZ                      = 25 #TBD
PREPARE_AND_EXECUTE_TOGETHER              = 26 #TBD

# The newest feature this driver supports.
# The server will negotiate the lowest compatible version.
CURRENT_PROTOCOL_MAJOR     = 1
CURRENT_PROTOCOL_VERSION   = XA_TRANSACTIONS
AUTH_TEST_STR              = 'Success!'
