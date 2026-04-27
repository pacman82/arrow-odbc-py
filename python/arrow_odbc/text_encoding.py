from enum import Enum


class TextEncoding(Enum):
    """
    Text encoding used for the payload of text columns, to transfer data from the data source to the
    application.

    ``Auto`` evaluates to Utf16 on windows and Self::Utf8 on other systems. We do this, because most
    systems e.g. MacOs and Linux use UTF-8 as their default encoding, while windows may still use a
    Latin1 or some other extended ASCII as their narrow encoding. On the other hand many Posix
    drivers are lacking in their support for wide function calls and UTF-16. So using ``Utf16`` on
    windows and ``Utf8`` everythere else is a good starting point.

    ``Utf8`` use narrow characters (one byte) to encode text in payloads. ODBC lets the client
    choose the encoding which should be based on the system local. This is often not what is
    actually happening though. If we use narrow encoding, we assume the text to be UTF-8 and error
    if we find that not to be the case.

    ``Utf16`` use wide characters (two bytes) to encode text in payloads. ODBC defines the encoding
    to be always UTF-16.
    """

    AUTO = 0
    UTF8 = 1
    UTF16 = 2
