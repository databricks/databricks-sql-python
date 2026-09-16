#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Row class was taken from Apache Spark pyspark.

from typing import Any, Dict, List, Optional, Tuple, Union, TypeVar
import datetime
import decimal
from ssl import SSLContext, CERT_NONE, CERT_REQUIRED, create_default_context

from databricks.sql.exc import ProgrammingError


class SSLOptions:
    tls_verify: bool
    tls_verify_hostname: bool
    tls_trusted_ca_file: Optional[str]
    tls_client_cert_file: Optional[str]
    tls_client_cert_key_file: Optional[str]
    tls_client_cert_key_password: Optional[str]

    def __init__(
        self,
        tls_verify: bool = True,
        tls_verify_hostname: bool = True,
        tls_trusted_ca_file: Optional[str] = None,
        tls_client_cert_file: Optional[str] = None,
        tls_client_cert_key_file: Optional[str] = None,
        tls_client_cert_key_password: Optional[str] = None,
    ):
        self.tls_verify = tls_verify
        self.tls_verify_hostname = tls_verify_hostname
        self.tls_trusted_ca_file = tls_trusted_ca_file
        self.tls_client_cert_file = tls_client_cert_file
        self.tls_client_cert_key_file = tls_client_cert_key_file
        self.tls_client_cert_key_password = tls_client_cert_key_password

    def validate_client_identity(self) -> None:
        """Validate the shape of the mutual-TLS client identity.

        ``SSLContext.load_cert_chain`` accepts a combined certificate + private-key
        PEM when ``keyfile`` is omitted, so a certificate without a separate key file
        is valid. The inverse is never useful: a key without a certificate would be
        silently ignored by the stdlib and downgrade the connection to one-way TLS.
        """
        if self.tls_client_cert_key_file and not self.tls_client_cert_file:
            raise ProgrammingError(
                "tls_client_cert_key_file (client private key) requires "
                "tls_client_cert_file (client certificate) for mutual TLS."
            )

    @staticmethod
    def _validate_client_identity_file(
        path: str, option_name: str, description: str
    ) -> None:
        """Reject an unreadable or zero-byte client-identity file clearly.

        Read only one byte: the PEM parser remains responsible for validating
        non-empty content, while this preflight can identify which of the two input
        paths failed before ``load_cert_chain`` collapses both into an opaque error.
        """
        try:
            with open(path, "rb") as file:
                has_content = bool(file.read(1))
        except OSError as exc:
            raise ProgrammingError(
                f"Failed to read {option_name} ({description}) '{path}' for mutual "
                f"TLS: {exc}"
            ) from exc

        if not has_content:
            raise ProgrammingError(
                f"{option_name} ({description}) '{path}' is empty; expected "
                "PEM-encoded content for mutual TLS."
            )

    def load_client_cert_chain(self, ssl_context: SSLContext) -> None:
        """Load the configured mutual-TLS identity into ``ssl_context``.

        Path validation intentionally precedes PEM parsing. Besides producing useful
        diagnostics, this ensures a missing/empty key is reported as the failing input
        even when the readable certificate file contains malformed non-empty bytes.
        """
        self.validate_client_identity()
        if not self.tls_client_cert_file:
            return

        self._validate_client_identity_file(
            self.tls_client_cert_file,
            "tls_client_cert_file",
            "client certificate",
        )
        if self.tls_client_cert_key_file:
            self._validate_client_identity_file(
                self.tls_client_cert_key_file,
                "tls_client_cert_key_file",
                "client private key",
            )

        ssl_context.load_cert_chain(
            certfile=self.tls_client_cert_file,
            keyfile=self.tls_client_cert_key_file,
            password=self.tls_client_cert_key_password,
        )

    def create_ssl_context(self) -> SSLContext:
        ssl_context = create_default_context(cafile=self.tls_trusted_ca_file)

        if self.tls_verify is False:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = CERT_NONE
        elif self.tls_verify_hostname is False:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = CERT_REQUIRED
        else:
            ssl_context.check_hostname = True
            ssl_context.verify_mode = CERT_REQUIRED

        self.load_client_cert_chain(ssl_context)

        return ssl_context


class Row(tuple):
    """
    A row in a query result.
    The fields in it can be accessed:

    * like attributes (``row.key``)
    * like dictionary values (``row[key]``)

    ``key in row`` will search through row keys.

    Row can be used to create a row object by using named arguments.
    It is not allowed to omit a named argument to represent that the value is
    None or missing. This should be explicitly set to None in this case.

    Examples
    --------
    >>> row = Row(name="Alice", age=11)
    >>> row
    Row(name='Alice', age=11)
    >>> row['name'], row['age']
    ('Alice', 11)
    >>> row.name, row.age
    ('Alice', 11)
    >>> 'name' in row
    True
    >>> 'wrong_key' in row
    False

    Row also can be used to create another Row like class, then it
    could be used to create Row objects, such as

    >>> Person = Row("name", "age")
    >>> Person
    <Row('name', 'age')>
    >>> 'name' in Person
    True
    >>> 'wrong_key' in Person
    False
    >>> Person("Alice", 11)
    Row(name='Alice', age=11)

    This form can also be used to create rows as tuple values, i.e. with unnamed
    fields.

    >>> row1 = Row("Alice", 11)
    >>> row2 = Row(name="Alice", age=11)
    >>> row1 == row2
    True
    """

    def __new__(cls, *args: Optional[str], **kwargs: Optional[Any]) -> "Row":
        if args and kwargs:
            raise ValueError("Can not use both args " "and kwargs to create Row")
        if kwargs:
            # create row objects
            row = tuple.__new__(cls, list(kwargs.values()))
            row.__fields__ = list(kwargs.keys())
            return row
        else:
            # create row class or objects
            return tuple.__new__(cls, args)

    def asDict(self, recursive: bool = False) -> Dict[str, Any]:
        """
        Return as a dict

        Parameters
        ----------
        recursive : bool, optional
            turns the nested Rows to dict (default: False).

        Notes
        -----
        If a row contains duplicate field names, e.g., the rows of a join
        between two dataframes that both have the fields of same names,
        one of the duplicate fields will be selected by ``asDict``. ``__getitem__``
        will also return one of the duplicate fields, however returned value might
        be different to ``asDict``.

        Examples
        --------
        >>> Row(name="Alice", age=11).asDict() == {'name': 'Alice', 'age': 11}
        True
        >>> row = Row(key=1, value=Row(name='a', age=2))
        >>> row.asDict() == {'key': 1, 'value': Row(name='a', age=2)}
        True
        >>> row.asDict(True) == {'key': 1, 'value': {'name': 'a', 'age': 2}}
        True
        """

        if not hasattr(self, "__fields__"):
            raise TypeError("Cannot convert a Row class into dict")

        if recursive:

            def conv(obj: Any) -> Any:
                if isinstance(obj, Row):
                    return obj.asDict(True)
                elif isinstance(obj, list):
                    return [conv(o) for o in obj]
                elif isinstance(obj, dict):
                    return dict((k, conv(v)) for k, v in obj.items())
                else:
                    return obj

            return dict(zip(self.__fields__, (conv(o) for o in self)))
        else:
            return dict(zip(self.__fields__, self))

    def __contains__(self, item: Any) -> bool:
        if hasattr(self, "__fields__"):
            return item in self.__fields__
        else:
            return super(Row, self).__contains__(item)

    # let object acts like class
    def __call__(self, *args: Any) -> "Row":
        """create new Row object"""
        if len(args) > len(self):
            raise ValueError(
                "Can not create Row with fields %s, expected %d values "
                "but got %s" % (self, len(self), args)
            )
        return _create_row(self, args)

    def __getitem__(self, item: Any) -> Any:
        if isinstance(item, (int, slice)):
            return super(Row, self).__getitem__(item)
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self.__fields__.index(item)
            return super(Row, self).__getitem__(idx)
        except IndexError:
            raise KeyError(item)
        except ValueError:
            raise ValueError(item)

    def __getattr__(self, item: str) -> Any:
        if item.startswith("__"):
            raise AttributeError(item)
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self.__fields__.index(item)
            return self[idx]
        except IndexError:
            raise AttributeError(item)
        except ValueError:
            raise AttributeError(item)

    def __setattr__(self, key: Any, value: Any) -> None:
        if key != "__fields__":
            raise RuntimeError("Row is read-only")
        self.__dict__[key] = value

    def __reduce__(
        self,
    ) -> Union[str, Tuple[Any, ...]]:
        """Returns a tuple so Python knows how to pickle Row."""
        if hasattr(self, "__fields__"):
            return (_create_row, (self.__fields__, tuple(self)))
        else:
            return tuple.__reduce__(self)

    def __repr__(self) -> str:
        """Printable representation of Row used in Python REPL."""
        if hasattr(self, "__fields__"):
            return "Row(%s)" % ", ".join(
                "%s=%r" % (k, v) for k, v in zip(self.__fields__, tuple(self))
            )
        else:
            return "<Row(%s)>" % ", ".join("%r" % field for field in self)


def _create_row(
    fields: Union["Row", List[str]], values: Union[Tuple[Any, ...], List[Any]]
) -> "Row":
    row = Row(*values)
    row.__fields__ = fields
    return row
