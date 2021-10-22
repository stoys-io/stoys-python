from dataclasses import dataclass
from typing import List


@dataclass
class PmfBucket:
    low: float
    high: float
    count: int


@dataclass
class Pmf:
    pmf: List[PmfBucket]

    def _repr_html_(self) -> str:
        from .ui.pmf_ui import pmf_to_html

        return pmf_to_html(self)
