"""
Does sql.connect_db.transaction() really run its block in one transaction?

It sets `conn.autocommit = False`, but in aiomysql `autocommit` is a coroutine
method, so that line only shadows the method on the connection object and
never reaches the server. If the pool's autocommit=True survives, every
statement in the block commits on its own and the rollback undoes nothing.

Runs against the test stack's MariaDB (port 13306):
  python test/check_transaction_autocommit.py
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.update(DB_HOST="127.0.0.1", DB_PORT="13306", DB_USER="psyduck", DB_PASSWORD="psyduck",
                  DB_NAME="PsyduckV2", LOG_FILE="False")

import sql.connect_db as db  # noqa: E402


async def main() -> int:
    await db.init_db()
    await db.execute("CREATE TABLE IF NOT EXISTS _txn_probe (id INT PRIMARY KEY) ENGINE=InnoDB")
    await db.execute("DELETE FROM _txn_probe")

    async with db.transaction(dict_cursor=False) as cur:
        await cur.execute("SELECT @@autocommit")
        session_autocommit = (await cur.fetchone())[0]

    try:
        async with db.transaction(dict_cursor=False) as cur:
            await cur.execute("INSERT INTO _txn_probe VALUES (1)")
            raise RuntimeError("force rollback")
    except RuntimeError:
        pass
    survived = await db.fetch_val("SELECT COUNT(*) FROM _txn_probe")
    await db.execute("DROP TABLE _txn_probe")
    await db.close_db()

    print(f"@@autocommit inside transaction(): {session_autocommit}")
    print(f"rows left after a rolled-back insert: {survived}")
    ok = session_autocommit == 0 and survived == 0
    print("transaction() is atomic:", "YES" if ok else "NO - each statement commits on its own")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
