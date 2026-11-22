"""The sample CSV files must stay well-formed: UTF-8, one value per column."""

import csv
from pathlib import Path

import pytest

DATA = Path(__file__).resolve().parent.parent / "data"


@pytest.mark.parametrize("path", sorted(DATA.rglob("*.csv")), ids=lambda path: path.name)
def test_every_row_has_as_many_values_as_the_header(path):
    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))
    assert len(rows) > 1
    assert {len(row) for row in rows} == {len(rows[0])}
