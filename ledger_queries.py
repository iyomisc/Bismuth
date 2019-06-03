"""
This module hosts LedgerQueries, a class grouping all data extraction queries from the DB.
It is closely related to db handler, as it relies on the same db engine and structure.
This module only make use of a single DB handler, and suppose all data will be taken from that one.
In the context of wallet servers or hypernodes, this supposes full ledger, on disk.

The goal of this class is to provide optimized requests to be used by plugins,
so they don't have to deal with low level, and possibly changing, db structure.

Borrows code from Hypernodes, pow_interface
"""
import math
# from typing import Union

__version__ = "0.0.2"


SQL_BLOCK_HEIGHT_PRECEDING_TS_SLOW = (
    "SELECT block_height FROM transactions WHERE timestamp <= ? "
    "ORDER BY block_height DESC limit 1"
)

SQL_BLOCK_HEIGHT_PRECEDING_TS = (
    "SELECT max(block_height) FROM transactions WHERE timestamp <= ? AND reward > 0"
)

SQL_TS_OF_BLOCK = (
    "SELECT timestamp FROM transactions WHERE reward > 0 AND block_height = ?"
)

SQL_REGS_FROM_TO = (
    "SELECT block_height, address, operation, openfield, timestamp FROM transactions "
    "WHERE (operation='hypernode:register' OR operation='hypernode:unregister') "
    "AND block_height >= ? AND block_height <= ? "
    "ORDER BY block_height ASC"
)

SQL_QUICK_BALANCE_CREDITS = "SELECT sum(amount+reward) FROM transactions WHERE recipient = ? AND block_height <= ?"

SQL_QUICK_BALANCE_DEBITS = (
    "SELECT sum(amount+fee) FROM transactions WHERE address = ? AND block_height <= ?"
)

SQL_QUICK_BALANCE_ALL = (
    "SELECT sum(a.amount+a.reward)-debit FROM transactions as a , "
    "(SELECT sum(b.amount+b.fee) as debit FROM transactions b "
    "WHERE address = ? AND block_height <= ?) "
    "WHERE a.recipient = ? AND a.block_height <= ?"
)

SQL_QUICK_BALANCE_ALL_MIRROR = (
    "SELECT sum(a.amount+a.reward)-debit FROM transactions as a , "
    "(SELECT sum(b.amount+b.fee) as debit FROM transactions b "
    "WHERE address = ? AND abs(block_height) <= ?) "
    "WHERE a.recipient = ? AND abs(a.block_height) <= ?"
)

SQL_LAST_BLOCK_TS = (
    "SELECT timestamp FROM transactions WHERE block_height = "
    "(SELECT max(block_height) FROM transactions)"
)


class LedgerQueries:

    @classmethod
    def execute(
        cls,
        db,
        sql: str,
        param: tuple = None,
        many: bool = False,
    ):
        """
        Safely execute the request

        :param db:
        :param sql:
        :param param:
        :param many: If True, will use an executemany call with param being a list of params.
        :return: cursor
        """
        tries = 0
        while True:
            try:
                if many:
                    cursor = db.executemany(sql, param)
                elif param:
                    cursor = db.execute(sql, param)
                else:
                    cursor = db.execute(sql)
                break
            except Exception as e:
                self.app_log.warning("Database query {}: {}".format(self.db_name, sql))
                self.app_log.warning("Database retry reason: {}".format(e))
                tries += 1
                if tries >= 10:
                    self.app_log.error("Database Error, closing")
                    # raise ValueError("Too many retries")
                    exit()
                time.sleep(0.1)
        return cursor

    @classmethod
    def fetchone(cls, db, sql: str, param: tuple = None, as_dict: bool = False):
        """
        Fetch one and Returns data.

        :param db:
        :param sql:
        :param param:
        :param as_dict: returns result as a dict, default False.
        :return: tuple()
        """
        cursor = cls.execute(db, sql, param)
        data = cursor.fetchone()
        if not data:
            return None
        if as_dict:
            return dict(data)
        return tuple(data)

    @classmethod
    def reg_check_weight(cls, db, address: str, height: int) -> int:
        """
        Calc rough estimate (not up to 1e-8) of the balance of an account at a certain point in the past.
        Raise if not enough for an HN, or return the matching Weight.

        Requires a full ledger.

        :param db: db cursor
        :param address:
        :param height:
        :return: weight (1, 2 or 3)
        """
        res = cls.fetchone(db, SQL_QUICK_BALANCE_ALL, (address, height, address, height))
        balance = res[0]
        weight = math.floor(balance/10000)
        if weight > 3:
            weight = 3
        return weight
