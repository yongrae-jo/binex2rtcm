"""Project-specific exceptions."""


class Binex2RtcmError(Exception):
    """Base exception for the converter."""


class ConfigurationError(Binex2RtcmError):
    """Raised when configuration is invalid."""


class StreamError(Binex2RtcmError):
    """Raised for stream transport failures."""


class ProtocolError(Binex2RtcmError):
    """Raised for malformed BINEX or RTCM payloads."""


class UnsupportedRecordError(ProtocolError):
    """Raised when a BINEX record is valid but unsupported."""


class UnsupportedMessageError(ProtocolError):
    """Raised when an RTCM message type is not implemented."""


class ValidationError(Binex2RtcmError):
    """Raised when produced RTCM bytes fail validation."""
