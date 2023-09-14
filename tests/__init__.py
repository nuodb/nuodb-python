"""
(C) Copyright 2013-2023 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import os
import string
import random
import socket
import tempfile
import logging
import time
import copy
import shutil
import errno
import unittest
import subprocess
import json

DB_OPTIONS = []

DATABASE_NAME = 'pynuodb_test'
DBA_USER      = 'dba'
DBA_PASSWORD  = 'dba_password'

ap_conn = None
ar_path = None
sql_host = None
db_created = False
db_started = False


_package_setup = False


def get_sqlhost():
    global sql_host
    return sql_host


def waitforsql(tmout):
    import pynuodb
    end = time.time() + tmout
    conn = None
    try:
        while True:
            try:
                conn = pynuodb.connect(database=DATABASE_NAME, host=get_sqlhost(),
                                       user=DBA_USER, password=DBA_PASSWORD,
                                       options={'schema': 'test'})
                return
            except pynuodb.session.SessionException:
                pass
            if time.time() > end:
                raise Exception("Timed out waiting for a TE to be ready")
            time.sleep(1)
    finally:
        if conn:
            conn.close()


def setUpPackage_pynuoadmin():
    """This method uses the pynuoadmin management interface to set up the DB.

    Unfortunately pynuoadmin is still Python2 only: until it supports Python3
    we can't use this interface, portably, since pynuodb supports P3 already.
    """
    from pynuoadmin import nuodb_mgmt

    global db_created, db_started
    global ap_conn

    # Use the same method of locating the AP REST service as nuocmd
    key = os.environ.get('NUOCMD_CLIENT_KEY')
    api = os.environ.get('NUOCMD_API_SERVER', 'localhost:8888')
    if not api.startswith('http://') and not api.startswith('https://'):
        if not key:
            api = 'http://' + api
        else:
            api = 'https://' + api

    ap_conn = nuodb_mgmt.AdminConnection(api, key)

    for db in ap_conn.get_databases():
        if db.name == DATABASE_NAME:
            logging.info("Reusing already-existing database %s" % (DATABASE_NAME))
            break

    # If we have a database and it's running, we're all set!
    if db and db.state != 'NOT_RUNNING':
        logging.info("Reusing already-running database %s" % (db.state))
        return

    # Find an AP running on the local host
    myhost = set(['localhost', socket.getfqdn(), socket.gethostname()])
    for ap in ap_conn.get_servers():
        hnm = ap.address.split(':', 1)[0]
        if hnm in myhost or hnm.split('.', 1)[0] in myhost:
            apid = ap.id
            localaddr = ap.address
            break
    else:
        raise Exception("Unable to locate a NuoDB AP on the local host")

    def create_archive():
        global ar_path
        chars = string.ascii_uppercase + string.digits
        ardir = '%s-%s' % (DATABASE_NAME,
                           ''.join(random.choice(chars) for x in range(20)))
        ar_path = os.path.join(tempfile.gettempdir(), ardir)
        logging.info("Creating archive %s" % (ar_path))
        ap_conn.create_archive(DATABASE_NAME, apid, ar_path)

    # If we have a database and it's not running, start it
    if db:
        logging.info('Starting NOT_RUNNING database %s' % (db.state))
        for ar in ap_conn.get_archives(db_name=DATABASE_NAME):
            ap_conn.start_process(DATABASE_NAME, apid, engine_type='SM',
                                  archive_id=ar.id,
                                  options=DB_OPTIONS)
        else:
            create_archive()
        ap_conn.start_process(DATABASE_NAME, apid, options=DB_OPTIONS)
    else:
        logging.info("Creating database %s" % (DATABASE_NAME))
        create_archive()
        ap_conn.create_database(DATABASE_NAME, DBA_USER, DBA_PASSWORD,
                                te_server_ids=[apid], options=DB_OPTIONS)
        db_created = True
        db = ap_conn.get_database(DATABASE_NAME)

    db_started = True
    logging.info("Wait for running: state %s" % (db.state))
    end = time.time() + 30
    while db.state != 'RUNNING':
        if time.time() > end:
            raise Exception("DB failed to go RUNNING")
        time.sleep(1)
        db = ap_conn.get_database(DATABASE_NAME)

    # There's no easy way to get the SQL client host/port of an AP
    global sql_host

    cfg = ap_conn.get_admin_config(apid)
    sql_host = cfg.properties.get('altAddr')
    if not sql_host:
        sql_host = localaddr.split(':')[0]
    if 'agentPort' in cfg.properties:
        sql_host += ':' + cfg.properties['agentPort']

    waitforsql(30)


def cvtjson(jstr):
    # Unfortunately the output of nuocmd is not a valid JSON object;
    # it's a dump of one or more JSON objects concatenated together.
    return json.loads(jstr if jstr.startswith('[') else
                      '[' + jstr.replace('\n}\n{', '\n},\n{') + ']')


# Python coverage's subprocess support breaks tests: nuocmd is a Python 2
# script which doesn't have access to the virtenv or whatever pynuodb is
# using.  So, nuocmd generates error messages related to coverage then the
# parsing of the JSON output fails.  Get rid of the coverage environment
# variables.
env_nocov = copy.copy(os.environ)
env_nocov.pop('COV_CORE_SOURCE', None)
env_nocov.pop('COV_CORE_CONFIG', None)


def nuocmd(args):
    proc = subprocess.Popen(['nuocmd'] + args, env=env_nocov,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    (out, _) = proc.communicate()
    return (proc.wait(), out.decode('UTF-8'))


def waitforstate(state, tmout):
    logging.info("Waiting %ds for state %s" % (tmout, state))
    end = time.time() + tmout
    while True:
        (ret, out) = nuocmd(['--show-json', 'get', 'database',
                             '--db-name', DATABASE_NAME])
        if ret != 0:
            raise Exception("Database get failed: %s" % (out))
        db = json.loads(out)
        if db.get('state') == state:
            logging.info("DB is %s" % (state))
            return
        if time.time() > end:
            raise Exception("Timed out waiting for %s" % (state))
        time.sleep(1)


def setUpPackage_nuocmd():
    """This method uses the nuocmd CLI interface to set up the DB.

    This works regardless of which Python we are using to run tests.
    """
    global db_created, db_started

    (ret, out) = nuocmd(['--show-json', 'get', 'servers'])
    if ret != 0:
        raise Exception("Cannot retrieve NuoDB AP servers: %s" % (out))
    myhost = set(['localhost', socket.getfqdn(), socket.gethostname()])
    for ap in cvtjson(out):
        hnm = ap.get('address', '').split(':', 1)[0]
        if hnm in myhost or hnm.split('.', 1)[0] in myhost:
            localap = ap.get('id')
            localaddr = ap.get('address')
            break
    if not localap:
        raise Exception("No NuoDB AP running on %s" % (str(myhost)))

    ar_id = None
    (ret, out) = nuocmd(['--show-json', 'get', 'database',
                         '--db-name', DATABASE_NAME])
    has_db = False
    if ret == 0:
        db = json.loads(out)
        if db and db.get('state') != 'TOMBSTONE':
            logging.info("Using existing database %s" % (DATABASE_NAME))
            has_db = True
            if db.get('state') == 'RUNNING':
                logging.info("Database is already RUNNING")
                return

    (ret, out) = nuocmd(['--show-json', 'get', 'archives',
                         '--db-name', DATABASE_NAME])
    if ret == 0:
        ars = cvtjson(out)
        if len(ars):
            ar_id = ars[0].get('id')

    if ar_id is None:
        global ar_path
        ardir = DATABASE_NAME + '-' + ''.join(random.choice(string.ascii_uppercase + string.digits)
                                              for x in range(20))
        ar_path = os.path.join(tempfile.gettempdir(), ardir)
        logging.info("Creating archive %s" % (ar_path))
        (ret, out) = nuocmd(['--show-json', 'create', 'archive',
                             '--db-name', DATABASE_NAME,
                             '--server-id', localap,
                             '--archive-path', ar_path])
        if ret != 0:
            raise Exception("Unable to create archive %s: %s" % (ar_path, out))
        ar = json.loads(out)
        ar_id = ar.get('id')

    if has_db:
        (ret, out) = nuocmd(['update', 'database-options',
                             '--db-name', DATABASE_NAME,
                             '--default-options'] + DB_OPTIONS)
        if ret != 0:
            raise Exception("Failed to reset database options")
        (ret, out) = nuocmd(['start', 'database', '--db-name', DATABASE_NAME,
                             '--te-server-ids', localap])
        if ret != 0:
            raise Exception("Failed to start database: %s" % (out))
    else:
        (ret, out) = nuocmd(['create', 'database', '--db-name', DATABASE_NAME,
                             '--dba-user', DBA_USER,
                             '--dba-password', DBA_PASSWORD,
                             '--te-server-ids', localap,
                             '--default-options'] + DB_OPTIONS)
        if ret != 0:
            raise Exception("Failed to create database: %s" % (out))
        db_created = True

    db_started = True

    waitforstate('RUNNING', 30)

    # There's no easy way to get the SQL client host/port of an AP
    global sql_host

    (ret, out) = nuocmd(['--show-json', 'get', 'server-config', '--this-server'])
    if ret != 0:
        raise Exception("Failed to get server config!")
    apinfo = json.loads(out)
    sql_host = str(apinfo['properties'].get('altAddr'))
    if not sql_host:
        sql_host = str(localaddr.split(':')[0])
    if 'agentPort' in apinfo['properties']:
        sql_host += ':' + str(apinfo['properties']['agentPort'])

    waitforsql(30)


def _cleanup():
    global ap_conn, db_created, db_started
    if ap_conn:
        if db_started:
            ap_conn.shutdown_database(DATABASE_NAME)
            if db_created:
                ap_conn.delete_database(DATABASE_NAME)
        ap_conn = None
    else:
        if db_started:
            db_started = False
            (ret, out) = nuocmd(['shutdown', 'database',
                                 '--db-name', DATABASE_NAME])
            if ret != 0:
                raise Exception("Failed to shutdown: %s" % (out))
            waitforstate('NOT_RUNNING', 30)
            if db_created:
                db_created = False
                (ret, out) = nuocmd(['delete', 'database',
                                     '--db-name', DATABASE_NAME])
                if ret != 0:
                    raise Exception("Failed to delete: %s" % (out))
    if ar_path:
        try:
            shutil.rmtree(ar_path)
        except OSError as ex:
            # If NuoDB Admin is running as "nuodb" we won't have permissions
            # to delete the archive so ignore it.
            if ex.errno != errno.EACCES:
                raise


# These are for pytest, which does run setUpModule from __init__.py

def setUpModule():
    global _package_setup
    if _package_setup:
        return
    _package_setup = True
    try:
        # Until pynuoadmin supports Python 3, use nuocmd
        setUpPackage_nuocmd()
    except Exception:
        _cleanup()
        raise


def tearDownModule():
    global _package_setup
    if not _package_setup:
        return
    _package_setup = False
    _cleanup()


# These are for unittest, which doesn't run setUpModule from __init__.py

def startTestRun(self):
    setUpModule()


def stopTestRun(self):
    tearDownModule()


setattr(unittest.TestResult, 'startTestRun', startTestRun)
setattr(unittest.TestResult, 'stopTestRun', stopTestRun)
