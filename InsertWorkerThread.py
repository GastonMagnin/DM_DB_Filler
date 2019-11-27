import json
import random
import re
from contextlib import closing

import mysql.connector
from PySide2.QtCore import QThread, Signal
from PySide2.QtWidgets import *
import urllib.request


def get_keywords():
    with open("keywords.txt") as f:
        x = f.readline()
        keywords = x.split(",")

    return keywords


class InsertWorkerThread(QThread):
    # define signals
    inserts_done = Signal(int)
    current_progress = Signal(int)
    def __init__(self):
        super().__init__()


    def run(self):
        self.insert_data()

    def set_data(self, db_data, table_name, vals, rows, amount):
        self.db_data = db_data
        self.table_name = table_name
        self.vals = vals
        self.rows = rows
        self.amount = amount

    def insert_data(self):
        keywords = get_keywords()
        amount = self.amount
        table_name = self.table_name
        vals = self.vals
        rows = self.rows
        data = []
        user_data = []
        succ_counter = 0
        try:
            with closing(mysql.connector.connect(**self.db_data)) as db:
                cursor = db.cursor()
                for i in range(len(vals)):
                    y = re.search("^random\(getuserdata\)$", vals[i])
                    if y is not None:
                        if not user_data:
                            for j in range(amount):
                                user_data.append(self.get_random_user_data())
                        continue
                    # check for keywords that don't need handling here
                    if [True for keyword in keywords if re.search(f"^random\({keyword}\)", vals[i]) is not None]:
                        continue
                    x = re.search("^random\((.*)\)", vals[i])
                    if x is not None:
                        stuff = x.group(1).split(".")
                        cursor.execute("SELECT %s FROM %s" % (stuff[1], stuff[0]))
                        data.append(cursor.fetchall())

                    # generate and execute the sql
                for i in range(amount):
                    # self.connection_bar.setValue(0)
                    try:
                        if user_data:
                            cursor.execute(self.insert_factory(table_name, vals, rows, data, user_data[i]))
                        else:
                            cursor.execute(self.insert_factory(table_name, vals, rows, data))
                        succ_counter += 1
                    except Exception as e:
                        print(e)
                        pass
                    self.current_progress.emit(i+1)
                    # self.connection_bar.setValue(100)
                db.commit()
                # self.connection_bar.setValue(0)
        except Exception as e:
            print(e.with_traceback())
            # show error and return
            # todo replace with signal
            QErrorMessage(self).showMessage("Connection failed")
            return

        self.inserts_done.emit(succ_counter)


    def insert_factory(self, table_name, vals, rows, data=None, user_data=None):
        print(data)
        counter = 0
        newvals = []
        for i in range(len(vals)):
            if re.search("^random\(getuserdata\)$", vals[i]) is not None:
                newvals.append(user_data[rows[i]])
            elif re.search("^random\((gibberish)\)", vals[i]) is not None:
                newvals.append(self.get_gibberish())
            elif re.search("^random\(rand_number\[([0-9]*)[,-:]([0-9]*)\]\)", vals[i]) is not None:
                x = re.search("^random\(rand_number\[([0-9]*)[,-:]([0-9]*)\]\)", vals[i])
                newvals.append(str(random.randint(int(x.group(1)), int(x.group(2)))))
            elif re.search("^random\((.*)\)", vals[i]) is not None:
                # [0] cursor fetchall returns tuples
                newvals.append(str(data[counter][random.randint(0, len(data[counter]) - 1)][0]))
                counter += 1
            else:
                newvals.append(vals[i])
        sql = "INSERT INTO %s%s VALUES (%s)" % (
            table_name, "" if not rows else "(" + (",".join(rows)) + ")", ", ".join("\"%s\"" % x for x in newvals))
        print(sql)
        return sql

    # noinspection SpellCheckingInspection
    def get_random_user_data(self):
        output = {}
        url = "https://randomuser.me/api"
        req = urllib.request.Request(url)
        try:
            with urllib.request.urlopen(req) as data:
                user_data = json.loads(data.read())["results"][0]
        except Exception as e:
            print(e.with_traceback())
            return
        output["handle"] = user_data["login"]["username"][0:15]
        output["anzeigename"] = " ".join(list(user_data["name"].values())[1:])
        output["email"] = user_data["email"]
        output["verifiziert"] = random.randint(0, 1)
        output["passwort"] = user_data["login"]["salt"]
        output["land_id"] = random.randint(1, 6)
        output["telefonnummer"] = user_data["cell"]
        output["geburtsdatum"] = user_data["dob"]["date"]
        output["beitrittsdatum"] = user_data["registered"]["date"]
        output["geburtsdatumssichtbarkeit"] = str(random.randint(1, 5))
        output["geburtsdatum_monat_und_tag_sichtbarkeit"] = str(random.randint(1, 5))
        output["ort"] = user_data["location"]["city"]
        # get gibberish text
        url = "https://www.randomtext.me/api/gibberish/p-1/15-25"
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:70.0) Gecko/20100101 Firefox/70.0"})
        try:
            with urllib.request.urlopen(req) as x:
                xdict = json.loads(x.read())
        except Exception as e:
            print(e)
            return
        output["bio"] = xdict["text_out"][3:-5]
        return output

    def get_gibberish(self):
        url = "https://www.randomtext.me/api/gibberish/p-1/15-25"
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:70.0) Gecko/20100101 Firefox/70.0"})
        try:
            with urllib.request.urlopen(req) as x:
                xdict = json.loads(x.read())
        except Exception as e:
            print(e.with_traceback())
            return
        return xdict["text_out"][3:-5]