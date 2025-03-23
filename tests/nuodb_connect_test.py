"""
(C) Copyright 2013-2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import socket
import pytest

import pynuodb
from pynuodb.exception import ProgrammingError
from pynuodb.session import SessionException

from . import nuodb_base
import pynuodb.crypt


class TestNuoDBConnect(nuodb_base.NuoBase):
    def test_nosuchdatabase(self):
        with pytest.raises(SessionException):
            pynuodb.connect("nosuchdatabase",
                            self.connect_args['host'],
                            self.connect_args['user'],
                            self.connect_args['password'])

    def test_nosuchport(self):
        # Different versions of Python give different exceptions here
        try:
            pynuodb.connect(self.connect_args['database'], "localhost:23456",
                            self.connect_args['user'],
                            self.connect_args['password'])
            pytest.fail("Connection to bogus port succeeded")
        except Exception:
            pass

    def test_nosuchhost(self):
        with pytest.raises(socket.gaierror):
            pynuodb.connect(self.connect_args['database'], "nosuchhost",
                            self.connect_args['user'],
                            self.connect_args['password'])

    def test_nosuchuser(self):
        with pytest.raises(ProgrammingError):
            pynuodb.connect(self.connect_args['database'],
                            self.connect_args['host'],
                            "nosuchuser",
                            self.connect_args['password'])

    def test_nosuchpassword(self):
        with pytest.raises(ProgrammingError):
            pynuodb.connect(self.connect_args['database'],
                            self.connect_args['host'],
                            self.connect_args['user'],
                            "nosuchpassword")

    @pytest.mark.skipif(not pynuodb.crypt.AESImported, reason="No AES available")
    def test_aes256cipher(self):
        conn = self._connect(options={'ciphers': 'AES-256-CTR'})
        try:
            config = conn.connection_config()
            assert config['cipher'] == 'AES-256'
            conn.testConnection()
        finally:
            conn.close()

    @pytest.mark.skipif(not pynuodb.crypt.AESImported, reason="No AES available")
    def test_aes128cipher(self):
        conn = self._connect(options={'ciphers': 'AES-128-CTR'})
        try:
            config = conn.connection_config()
            assert config['cipher'] == 'AES-128'
            conn.testConnection()
        finally:
            conn.close()

    def test_rc4cipher(self):
        conn = self._connect(options={'ciphers': 'RC4'})
        try:
            config = conn.connection_config()
            assert config['cipher'].startswith('RC4')
            conn.testConnection()
        finally:
            conn.close()
