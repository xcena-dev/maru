# SPDX-License-Identifier: Apache-2.0
"""Remote connection authentication and key files."""

import pytest

from maru_remote.auth import ConnectionAuth, KeyFileError, load_key, prove, write_key


def test_an_auth_reply_with_another_proof_is_refused():
    auth = ConnectionAuth(b"k" * 32)
    reply = auth.check(
        {
            "op": "auth",
            "proof": prove(b"k" * 32, "handler", auth.nonce),
            "nonce": b"c" * 16,
        }
    )
    assert reply["ok"] and auth.proved
    assert reply["proof"] != prove(b"other" * 7, "agent", b"c" * 16)


def test_the_key_file_must_hold_a_key_and_stay_private(tmp_path):
    path = tmp_path / "key"
    write_key(str(path))
    assert path.stat().st_mode & 0o777 == 0o600
    assert len(load_key(str(path))) == 32
    with pytest.raises(FileExistsError):
        write_key(str(path))  # an existing key is never replaced
    path.chmod(0o644)
    with pytest.raises(KeyFileError, match="readable by every user"):
        load_key(str(path))
    path.chmod(0o640)
    assert len(load_key(str(path))) == 32
    bad = tmp_path / "bad"
    bad.write_text("not hex\n")
    bad.chmod(0o600)
    with pytest.raises(KeyFileError, match="does not hold"):
        load_key(str(bad))
    with pytest.raises(KeyFileError, match="cannot read"):
        load_key(str(tmp_path / "missing"))


def test_writing_a_key_leaves_no_temporary_file(tmp_path):
    write_key(str(tmp_path / "key"))
    assert [p.name for p in tmp_path.iterdir()] == ["key"]
