import pathlib
import datetime
import sqlite3
import json
import subprocess
from dateutil.parser import parse

from beancount.core.number import D
from beancount.core import data
from beancount.core import amount
from beancount.ingest import importer

def flag(pending):
    if pending:
        return "!"
    
    return "*"

def map_to_beancount(txn, account_map, meta):
    date = parse(txn["date"]).date()
    desc = txn["name"]

    payee = txn.get("merchant_name", "")
    if payee == desc:
        payee = ""

    units = amount.Amount(round(D(txn["amount"]), 2), txn["iso_currency_code"])
    account = account_map.get(txn["account_id"], "Assets:FIXME")

    # Invert units on credit accounts.
    if is_credit_normal(account):
        units = -units

    posting1 = data.Posting("Expenses:FIXME", -units, None, None, None, None)
    posting2 = data.Posting(account, None, None, None, None, None)

    return data.Transaction(meta, date, flag(txn["pending"]), payee, desc,
            data.EMPTY_SET, data.EMPTY_SET, [posting1, posting2])


def try_sync(cmd):
    tries = 0
    while True:
        try:
            subprocess.run(cmd, check=True, shell=True, timeout=60)
        except:
            tries += 1
            if tries >= 3:
                break


class Importer(importer.ImporterProtocol):
    currency = "USD"

    def __init__(self, clerk_bin="clerk", clerk_conf="", clerk_db="", perform_sync=False, account_map={}):
        self.account_map = account_map
        self.clerk_bin = clerk_bin
        self.clerk_conf = clerk_conf
        self.clerk_db = clerk_db
        self.perform_sync = perform_sync

    def identify(self, file):
        path = pathlib.Path(file.name)

        if path.suffix == ".json" and "clerk" in path.name:
            return True

        return False
    
    def extract(self, file):
        with open(file.name, encoding="utf-8") as f:
            config = json.load(f)

        now = datetime.date.today()
        default_start = now - datetime.timedelta(days=14)
        start = config.get("start", default_start.isoformat())
        end = config.get("end", now.isoformat())
        perform_sync = config.get("perform_sync", False) or self.perform_sync

        if perform_sync:
            try_sync(f"{self.clerk_bin} --config {self.clerk_conf} txn sync")

        con = sqlite3.connect(self.clerk_db)
        rows = con.execute(f"SELECT source, JSON_EXTRACT(source, '$.date') as date FROM transactions WHERE date BETWEEN date(\"{start}\") AND date(\"{end}\")").fetchall()

        meta = data.new_metadata(file.name, 0)

        txns = []
        for (source) in rows:
            txn = json.loads(source[0])
            txns.append(map_to_beancount(txn, self.account_map, meta))

        return txns

def is_credit_normal(account_name):
    if account_name.startswith("Liabilities"):
        return True

    return False
