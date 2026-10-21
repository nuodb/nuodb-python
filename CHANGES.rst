Release notes
=============

5.0.0
-----

Changes since 4.1.0
~~~~~~~~~~~~~~~~~~~

* Require Python >= 3.9; remove Python 2 and older Python 3 compatibility.
* Require ``cryptography>=36.0`` as a runtime dependency instead of the optional
  ``[crypto]`` extra. Use the standard-library ``zoneinfo`` module instead of
  the older ``pytz`` compatibility path.
* Add optional Cython acceleration for result decoding, result-set handling,
  batch parameter encoding, and batch result decoding. Keep pure-Python
  implementations available when the extension is not installed.
* Reduce per-row overhead in fetching, socket reads, value dispatch, and
  cryptographic byte conversions.
* Reduce server round trips by preparing and executing a statement together
  when supported. Fetch ``Cursor.description`` metadata on first access rather
  than during every query execution.
* Speed up date/time conversions while retaining Julian/Gregorian fallbacks.
  Correct DATE decoding for session timezones east of UTC.
* Use ``secrets`` instead of ``random`` for client authentication private keys.
* Add opt-in insert/select performance benchmarks and binary-wheel build
  configuration.

Known issues
~~~~~~~~~~~~

* The accelerated ``executemany()`` encoder rejects a tuple of parameter rows,
  although the pure-Python implementation accepts it. The existing README
  example uses this form. A fix and regression tests are proposed in
  `PR #215 <https://github.com/nuodb/nuodb-python/pull/215>`_.
