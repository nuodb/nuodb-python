"""
(C) Copyright 2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import os
import pytest
import random
import string
import tempfile
import time
import socket
import logging
import base64
import shutil

try:
    from typing import Any, Generator, List  # pylint: disable=unused-import
    from typing import Mapping, NoReturn, Optional, Tuple  # pylint: disable=unused-import

    AP_FIXTURE = Tuple[str, str]
    ARCHIVE_FIXTURE = Tuple[str, str]
    DB_FIXTURE = Tuple[str, str, str]
    TE_FIXTURE = str
    DATABASE_FIXTURE = Mapping[str, Any]
except ImportError:
    pass

from . import nuocmd, cvtjson

_log = logging.getLogger("pynuodbtest")

DB_OPTIONS = []  # type: List[str]

DATABASE_NAME = 'pynuodb_test'
DBA_USER      = 'dba'
DBA_PASSWORD  = 'dba_password'

_CHARS = string.ascii_lowercase + string.digits

# Unfortunately purging the DB also purges the archive so we have to remember
# this externally from the archive fixture.
__archive_created = False


def __fatal(msg):
    pytest.exit(msg, returncode=1)


def waitforstate(dbname, state, tmout):
    # type: (str, str, float) -> None
    """Wait TMOUT seconds for database DBNAME to reach STATE."""
    _log.info("Waiting for db %s to reach state %s", dbname, state)
    end = time.time() + tmout
    while True:
        (ret, out) = nuocmd(['--show-json', 'get', 'database', '--db-name', dbname])
        assert ret == 0, "get database failed: %s" % (out)

        now = cvtjson(out)[0].get('state')
        if now == state:
            _log.info("DB %s is %s", dbname, state)
            return

        if time.time() > end:
            raise Exception("Timed out waiting for %s" % (state))
        time.sleep(1)


@pytest.fixture(scope="session")
def ap():
    # type: () -> Generator[AP_FIXTURE, None, None]
    """Find a running AP.  It must be started before running tests."""
    _log.info("Retrieving servers")
    (ret, out) = nuocmd(['--show-json', 'get', 'servers'])
    if ret != 0:
        __fatal("Cannot retrieve NuoDB AP servers: %s" % (out))

    myhost = set(['localhost', socket.getfqdn(), socket.gethostname()])
    for ap in cvtjson(out):
        hnm = ap.get('address', '').split(':', 1)[0]
        if hnm in myhost or hnm.split('.', 1)[0] in myhost:
            localap = ap.get('id')
            break
    if not localap:
        __fatal("No NuoDB AP running on %s" % (str(myhost)))

    # The only way to know the SQL address is via server-config
    (ret, out) = nuocmd(['--show-json', 'get', 'server-config', '--server-id', localap])
    if ret != 0:
        __fatal("Failed to retrieve config for server %s: %s" % (localap, out))
    cfg = cvtjson(out)[0]
    localaddr = '%s:%s' % (cfg['properties']['altAddr'], cfg['properties']['agentPort'])

    # We'll assume that any license at all is enough to run the minimum
    # database needed by the tests.
    def check_license():
        # type: () -> Optional[str]
        (ret, out) = nuocmd(['--show-json', 'get', 'effective-license'])
        if ret != 0:
            return "Cannot retrieve NuoDB domain license: %s" % (out)
        lic = cvtjson(out)[0]
        if not lic or 'decodedLicense' not in lic or 'type' not in lic['decodedLicense']:
            return "Invalid license: %s" % (out)
        if lic['decodedLicense']['type'] == 'UNLICENSED':
            return "NuoDB domain is UNLICENSED"

        return None

    _log.info("Checking licensing")
    err = check_license()

    # If we need a license and one exists in the environment, install it
    if err and os.environ.get('NUODB_LIMITED_LICENSE_CONTENT'):
        licfile = 'nuodb%s.lic' % (''.join(random.choice(_CHARS) for x in range(10)))
        licpath = os.path.join(tempfile.gettempdir(), licfile)
        _log.info("Adding a license provided by the environment")
        with open(licpath, 'wb') as f:
            f.write(base64.b64decode(os.environ['NUODB_LIMITED_LICENSE_CONTENT']))
        (ret, out) = nuocmd(['set', 'license', '--license-file', licpath])
        try:
            os.remove(licpath)
        except Exception:
            pass
        if ret != 0:
            __fatal("Failed to set a license: %s" % (out))

        err = check_license()

    if err:
        __fatal(err)

    yield localap, localaddr


@pytest.fixture(scope="session")
def archive(request, ap):
    # type: (pytest.FixtureRequest, AP_FIXTURE) -> Generator[ARCHIVE_FIXTURE, None, None]
    """Find or create an archive.

    :return path, id
    """
    localap, _ = ap
    global __archive_created

    _log.info("Retriving an archive")
    (ret, out) = nuocmd(['--show-json', 'get', 'archives',
                         '--db-name', DATABASE_NAME])
    ar_id = None
    if ret == 0:
        ars = cvtjson(out)
        if len(ars):
            ar_id = ars[0]['id']
            ar_path = ars[0]['path']
            _log.info("Using existing archive %s: %s", ar_id, ar_path)

    if not ar_id:
        ardir = DATABASE_NAME + '-' + ''.join(random.choice(_CHARS) for x in range(20))
        ar_path = os.path.join(tempfile.gettempdir(), ardir)
        _log.info("Creating archive %s", ar_path)
        (ret, out) = nuocmd(['--show-json', 'create', 'archive',
                             '--db-name', DATABASE_NAME,
                             '--server-id', localap,
                             '--archive-path', ar_path])
        if ret != 0:
            __fatal("Unable to create archive %s: %s" % (ar_path, out))
        ar = cvtjson(out)[0]
        ar_id = ar.get('id')
        __archive_created = True

    yield ar_path, ar_id

    if __archive_created:
        (ret, out) = nuocmd(['delete', 'archive', '--archive-id', str(ar_id)])
        assert ret == 0, "Failed to delete archive %s: %s" % (ar_id, out)

    # If nothing failed then delete the archive, else leave it for forensics
    if request.session.testsfailed == 0:
        shutil.rmtree(ar_path, ignore_errors=True)


@pytest.fixture(scope="session")
def get_db(archive):
    # type: (ARCHIVE_FIXTURE) -> Generator[DB_FIXTURE, None, None]
    _log.info("Retrieving database %s", DATABASE_NAME)
    (ret, out) = nuocmd(['--show-json', 'get', 'database',
                         '--db-name', DATABASE_NAME])
    created = True
    if ret == 0:
        db = cvtjson(out)
        if db and db[0].get('state') != 'TOMBSTONE':
            _log.info("Using existing database %s", DATABASE_NAME)
            (ret, out) = nuocmd(['update', 'database-options',
                                 '--db-name', DATABASE_NAME,
                                 '--default-options'] + DB_OPTIONS)
            if ret != 0:
                __fatal("Failed to reset database options: %s" % (out))
            # We assume that the correct username / password are configured!
            # This is to support fast test cycles: pre-creating the database
            # avoids the overhead of creating it for each test run.
            created = False

    if created:
        _log.info("Creating database %s", DATABASE_NAME)
        (ret, out) = nuocmd(['create', 'database', '--db-name', DATABASE_NAME,
                             '--no-autostart',
                             '--dba-user', DBA_USER,
                             '--dba-password', DBA_PASSWORD,
                             '--default-options'] + DB_OPTIONS)
        if ret != 0:
            __fatal("Failed to create database %s: %s" % (DATABASE_NAME, out))

    yield DATABASE_NAME, DBA_USER, DBA_PASSWORD

    if created:
        _log.info("Deleting database %s", DATABASE_NAME)
        (ret, out) = nuocmd(['delete', 'database', '--purge', '--db-name', DATABASE_NAME])
        assert ret == 0, "Failed to delete %s: %s" % (DATABASE_NAME, out)
        global __archive_created
        __archive_created = False


@pytest.fixture(scope="session")
def db(get_db):
    # type: (DB_FIXTURE) -> Generator[DB_FIXTURE, None, None]
    dbname = get_db[0]

    was_running = False

    (ret, out) = nuocmd(['--show-json', 'get', 'database', '--db-name', dbname])
    if ret != 0:
        __fatal("Failed to get db state %s: %s" % (dbname, out))
    state = cvtjson(out)[0]['state']
    if state == 'TOMBSTONE':
        __fatal("Database %s has exited: %s" % (dbname, out))
    if state == 'RUNNING':
        was_running = True
        _log.info("Database %s is already running", dbname)
    else:
        (ret, out) = nuocmd(['start', 'database', '--db-name', dbname])
        if ret != 0:
            __fatal("Failed to start database: %s" % (out))
        waitforstate(dbname, 'RUNNING', 30)
        _log.info("Started database %s", dbname)

    yield get_db[0], get_db[1], get_db[2]

    if not was_running:
        (ret, out) = nuocmd(['shutdown', 'database', '--db-name', dbname])
        assert ret == 0, "Failed to stop database %s: %s" % (dbname, out)
        waitforstate(dbname, 'NOT_RUNNING', 30)


@pytest.fixture(scope="session")
def te(ap, db):
    # type: (AP_FIXTURE, DB_FIXTURE) -> Generator[TE_FIXTURE, None, None]
    localap, _ = ap
    dbname = db[0]

    start_id = None
    started = False

    (ret, out) = nuocmd(['--show-json', 'get', 'processes', '--db-name', dbname])
    if ret != 0:
        __fatal("Failed to get db processes %s: %s" % (dbname, out))
    for proc in cvtjson(out):
        if proc['state'] == 'RUNNING' and proc['options']['engine-type'] == 'TE':
            start_id = proc['startId']
            _log.info("Using existing TE with sid:%s", start_id)
            break
    else:
        (ret, out) = nuocmd(['--show-json', 'start', 'process', '--db-name', dbname,
                             '--engine-type', 'TE', '--server-id', localap])
        if ret != 0:
            __fatal("Failed to start TE: %s" % (out))
        start_id = cvtjson(out)[0]['startId']
        started = True
        _log.info("Created a TE with sid:%s", start_id)

    yield start_id

    if started:
        (ret, out) = nuocmd(['shutdown', 'process', '--start-id', start_id])
        assert ret == 0, "Failed to stop TE %s: %s" % (start_id, out)


@pytest.fixture(scope='session')
def database(ap, db, te):
    # type: (AP_FIXTURE, DB_FIXTURE, TE_FIXTURE) -> DATABASE_FIXTURE
    import pynuodb
    end = time.time() + 30
    conn = None
    _log.info("Creating a SQL connection to %s as user %s with schema 'test'",
              db[0], db[1])

    connect_args = {'database': db[0],
                    'host': ap[1],
                    'user': db[1],
                    'password': db[2],
                    'options': {'schema': 'test'}}  # type: DATABASE_FIXTURE
    system_information = {'effective_version': 0}

    try:
        while True:
            try:
                conn = pynuodb.connect(**connect_args)
                cursor = conn.cursor()
                try:
                    cursor.execute("select GETEFFECTIVEPLATFORMVERSION() from system.dual")
                    row = cursor.fetchone()
                    system_information['effective_version'] = row[0]
                finally:
                    cursor.close()

                break
            except pynuodb.session.SessionException:
                pass
            if time.time() > end:
                raise Exception("Timed out waiting for a TE to be ready")
            time.sleep(1)
    finally:
        if conn:
            conn.close()

    _log.info("Database %s is available", db[0])

    return {'connect_args': connect_args, 'system_information': system_information}
