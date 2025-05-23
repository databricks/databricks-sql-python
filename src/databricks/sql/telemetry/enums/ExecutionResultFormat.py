from enum import Enum


class ExecutionResultFormat(Enum):
    FORMAT_UNSPECIFIED = "format_unspecified"
    INLINE_ARROW = "inline_arrow"
    EXTERNAL_LINKS = "external_links"
    COLUMNAR_INLINE = "columnar_inline"
