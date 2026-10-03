# SPDX-License-Identifier: Apache-2.0
"""Write buffer: loaded free pages handed out to stores."""

import pytest

from maru_remote.write_buffer import WriteBuffer


def test_wants_at_most_one_batch_and_the_missing_pages():
    wb = WriteBuffer(pages=3, refill_batch=2)
    assert wb.want() == 2
    done = [wb.loading(object(), (0, 1)), wb.loading(object(), (1, 1))]
    assert wb.want() == 0  # a batch is loading
    for d in done:
        d(True)
    assert wb.want() == 1  # 3 wanted, 2 ready


def test_take_hands_out_only_loaded_pages():
    wb = WriteBuffer(pages=4)
    pages = [(object(), (0, 1)), (object(), (1, 1))]
    for p, rng in pages:
        wb.loading(p, rng)(True)
    assert wb.take(3) is None
    assert wb.take(2) == pages
    assert wb.stats()["refused"] == 1 and wb.stats()["handed_out"] == 2


def test_failed_load_goes_back_to_the_server():
    wb = WriteBuffer(pages=2)
    page = object()
    wb.loading(page, (0, 1))(False)
    assert wb.take(1) is None
    assert wb.drain_orphans() == [(page, None)]  # nothing pinned to let go
    assert wb.drain_orphans() == []


def test_close_returns_ready_pages_and_later_loads():
    wb = WriteBuffer(pages=2)
    ready, late = object(), object()
    wb.loading(ready, (0, 1))(True)
    finish_late = wb.loading(late, (1, 1))
    assert wb.close() == [(ready, (0, 1))]
    finish_late(True)
    assert wb.drain_orphans() == [(late, (1, 1))]  # pinned: the server unpins it


@pytest.mark.parametrize("pages, batch", [(0, 1), (1, 0)])
def test_rejects_non_positive_settings(pages, batch):
    with pytest.raises(ValueError):
        WriteBuffer(pages, batch)
