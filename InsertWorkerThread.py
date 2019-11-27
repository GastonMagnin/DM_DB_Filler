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
        """
        Takes lists of columns and the values entered for these columns, checks for special inputs like random(getuserdata) and
        provides the data needed to replace them, then generates {amount} sql statements using the inputs and executes them
        Sends a signal to the main ui thread when it's done containing the number of successful inserts
        :return:
        """
        # initialize lists
        keywords = get_keywords()
        amount = self.amount
        table_name = self.table_name
        vals = self.vals
        columns = self.rows
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
                        if len(stuff) > 2:
                            sql = "SELECT %s FROM %s WHERE %s" % (stuff[1], stuff[0], stuff[2])
                            print(sql)
                            cursor.execute(sql)
                        else:
                            cursor.execute("SELECT %s FROM %s" % (stuff[1], stuff[0]))
                        data.append(cursor.fetchall())

                    # generate and execute the sql
                for i in range(amount):
                    # self.connection_bar.setValue(0)
                    try:
                        if user_data:
                            cursor.execute(self.insert_factory(table_name, vals, columns, data, user_data[i]))
                        else:
                            cursor.execute(self.insert_factory(table_name, vals, columns, data))
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


    def insert_factory(self, table_name, vals, columns, data=None, user_data=None):
        """
        Builds a SQL Insert String using the passed data, replaces special inputs like random(getuserdata) with the
        correct replacement
        :param table_name: the name of the table into which the data is to be inserted
        :param vals: the values (may contain special inputs) to be written to the table(after special inputs are replaced)
        :param columns: the columns into which the values are to be inserted
        :param data: (optional) if there are special inputs containing a table and columnname the contents of these columns will be passed here
        :param user_data: (optional) if there are special inputs in the form of random(getuserdata) a dict containing data for (almost) every column will be passed here
        :return: SQL String built with the passed data
        """
        counter = 0
        newvals = []
        # check for special inputs and replace them
        for i in range(len(vals)):
            # check for random(getuserdata)
            if re.search("^random\(getuserdata\)$", vals[i]) is not None:
                newvals.append(user_data[columns[i]])
            # check for random(gibberish)
            elif re.search("^random\(gibberish(?:\[([0-9]*)[,:-]([0-9]*)\])?\)", vals[i]) is not None:
                x = re.search("^random\(gibberish(?:\[([0-9]*)[,:-]([0-9]*)\])?\)", vals[i])
                if x.group(1) is not None and x.group(2) is not None:
                    newvals.append(self.get_gibberish(int(x.group(1)), int(x.group(2))))
                    continue
                newvals.append(self.get_gibberish())
            # check for random(rand_number[])
            elif re.search("^random\(rand_number\[([0-9]*)[,-:]([0-9]*)\]\)", vals[i]) is not None:
                x = re.search("^random\(rand_number\[([0-9]*)[,-:]([0-9]*)\]\)", vals[i])
                newvals.append(str(random.randint(int(x.group(1)), int(x.group(2)))))
            # check for random(tablename.columnname)
            elif re.search("^random\((.*)\)", vals[i]) is not None:
                # [0] cursor fetchall returns tuples
                newvals.append(str(data[counter][random.randint(0, len(data[counter]) - 1)][0]))
                counter += 1
            else:
                # if the line has no special inputs use the line itself
                newvals.append(vals[i])
        # build sql string
        sql = "INSERT INTO %s%s VALUES (%s)" % (
            table_name, "" if not columns else "(" + (",".join(columns)) + ")", ", ".join("\"%s\"" % x for x in newvals))
        print(sql)
        return sql

    # noinspection SpellCheckingInspection
    def get_random_user_data(self):
        """
        gets random user data from an api and builds a dict with relevant values
        :return:
        """
        # initialize output
        output = {}
        # build request
        url = "https://randomuser.me/api"
        req = urllib.request.Request(url)
        # get json from api and convert it
        try:
            with urllib.request.urlopen(req) as data:
                user_data = json.loads(data.read())["results"][0]
        except Exception as e:
            print(e.with_traceback())
            return
        # build output dict from json
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
        output["bio"] = self.get_gibberish()
        return output

    def get_gibberish(self, num=15, num2=25):
        """
        Gets gibberish text from an api
        :return: String containing gibberish text
        """
        # build request
        url = f"https://www.randomtext.me/api/gibberish/p-1/{num}-{num2}"
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:70.0) Gecko/20100101 Firefox/70.0"})
        # get json from api
        try:
            with urllib.request.urlopen(req) as x:
                xdict = json.loads(x.read())
        except Exception as e:
            print(e.with_traceback())
            return
        # extract the text
        return xdict["text_out"][3:-5]