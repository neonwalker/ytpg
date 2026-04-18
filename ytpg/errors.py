from __future__ import annotations


class PGError(Exception):
    def __init__(
        self,
        message: str,
        code: str = "XX000",
        severity: str = "ERROR",
        hint: str | None = None,
        detail: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.severity = severity
        self.hint = hint
        self.detail = detail

    def __repr__(self) -> str:
        return f"PGError(code={self.code!r}, message={self.message!r})"
