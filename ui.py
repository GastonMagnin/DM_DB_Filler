from contextlib import closing
import json
import re
import random
import sys
import urllib.request
# noinspection PyUnresolvedReferences
from PySide2.QtCore import QThread, SIGNAL, Slot

import resources
import mysql.connector
from PySide2.QtWidgets import *
from PySide2.QtGui import QIcon
from InsertWorkerThread import InsertWorkerThread

class DBFillerWidget(QWidget):

    def __init__(self):
        super().__init__()
        self.resize(800, 600)
        self.setWindowIcon(QIcon(":/resources/schmueckalf.png"))
        self.setWindowTitle("DBFiller")
        self.layout = QHBoxLayout()
        self.setLayout(self.layout)
        self.leftSideLayout = QVBoxLayout()
        self.layout.addLayout(self.leftSideLayout)
        self.rightSideLayout = QVBoxLayout()
        self.layout.addLayout(self.rightSideLayout)
        # left side elements
        # initialize go button
        self.go_button = QPushButton("Go")
        self.go_button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        # Select elements
        self.db_input_layout = QFormLayout()
        self.db_input_layout.setFieldGrowthPolicy(QFormLayout.FieldsStayAtSizeHint)
        self.leftSideLayout.addLayout(self.db_input_layout)
        self.table_name_label = QLabel("Table Name")
        self.table_name_input = QLineEdit()
        self.table_name_input.returnPressed.connect(self.go_button.click)
        self.table_name_input.setMaximumHeight(20)
        self.db_input_layout.addRow(self.table_name_label, self.table_name_input)
        self.column_input_label = QLabel("column")
        self.column_input = QLineEdit("*")
        self.column_input.returnPressed.connect(self.go_button.click)
        self.db_input_layout.addRow(self.column_input_label, self.column_input)
        self.condition_input_label = QLabel("condition")
        self.condition_input = QLineEdit("1")
        self.condition_input.returnPressed.connect(self.go_button.click)
        self.db_input_layout.addRow(self.condition_input_label, self.condition_input)
        # connect go button
        self.leftSideLayout.addWidget(self.go_button)
        self.go_button.clicked.connect(self.execute_select)
        # Insert Elements
        self.db_insert_input_layout = QFormLayout()
        self.leftSideLayout.addLayout(self.db_insert_input_layout)
        self.table_input_label = QLabel("table")
        self.table_input = QComboBox()
        self.table_input.currentIndexChanged.connect(self.generate_row_area)
        self.db_insert_input_layout.addRow(self.table_input_label, self.table_input)
        # rows
        self.row_area = QScrollArea()
        self.row_area.setWidgetResizable(True)
        self.row_area_frame = QFrame()
        self.row_layout = QFormLayout()
        self.row_area_frame.setLayout(self.row_layout)
        self.leftSideLayout.addWidget(self.row_area)
        self.row_area.setWidget(self.row_area_frame)
        # insert button
        self.amount_layout = QHBoxLayout()
        self.amount_input_label = QLabel("amount")
        self.amount_input_label.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Minimum)
        self.amount_input = QSpinBox()
        self.amount_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.amount_input.setRange(1, 1000)
        self.amount_layout.addWidget(self.amount_input_label)
        self.amount_layout.addWidget(self.amount_input)
        self.leftSideLayout.addLayout(self.amount_layout)
        self.insert_button = QPushButton("Insert")
        self.insert_button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.leftSideLayout.addWidget(self.insert_button)
        self.insert_button.clicked.connect(self.insert_data)
        # connect elements
        self.connect_elements_layout = QHBoxLayout()
        self.connect_button = QPushButton("Connect")
        self.connect_button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.connect_button.setMaximumHeight(20)
        self.connect_button.clicked.connect(self.connect_to_database)
        self.connection_bar = QProgressBar()
        self.connection_bar.setSizePolicy(QSizePolicy.MinimumExpanding, QSizePolicy.Fixed)
        self.connection_bar.setMaximumHeight(20)
        self.connect_elements_layout.addWidget(self.connect_button)
        self.connect_elements_layout.addWidget(self.connection_bar)
        self.leftSideLayout.addLayout(self.connect_elements_layout)

        # table
        self.data_table = QTableWidget(12, 3)
        self.data_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.rightSideLayout.addWidget(self.data_table)
        self.db_data = None
        # connect
        # self.connect_to_database()
        # shortcuts
        self.table_shortcut = QShortcut("Alt+t", self)
        self.table_shortcut.activated.connect(self.table_name_input.setFocus)
        self.connect_shortcut = QShortcut("Alt+c", self)
        self.connect_shortcut.activated.connect(self.connect_to_database)
        # threads
        self.worker_thread = None



    def connect_to_database(self):
        status, data = DBInputDialog.get_data(self)
        if status == 1:
            try:
                self.db_data = data
                # test connection
                with closing(mysql.connector.connect(**self.db_data)) as db:
                    self.connection_bar.setValue(100)
            except Exception as e:
                # show error message and reset the data
                QErrorMessage(self).showMessage("Connection failed")
                self.db_data = None
                return
            self.connection_bar.setValue(0)
            with closing(mysql.connector.connect(**data)) as db:
                # fill bar
                self.connection_bar.setValue(100)
                cursor = db.cursor()
                cursor.execute("SHOW tables")
                tables = cursor.fetchall()

            for table in tables:
                self.table_input.addItem(table[0])
            self.generate_row_area()
            self.connection_bar.setValue(0)

    def execute_select(self):
        if self.db_data is None or self.table_name_input.displayText() == "":
            return
        # build sql query
        sql = "SELECT %s FROM %s WHERE %s" % (
            self.column_input.displayText(), self.table_name_input.displayText(), self.condition_input.displayText())
        # execute sql query
        try:
            with closing(mysql.connector.connect(**self.db_data)) as db:
                self.connection_bar.setValue(20)
                cursor = db.cursor()
                cursor.execute(sql)
                data = cursor.fetchall()
        except:
            self.connection_bar.setValue(0)
            # show error and return
            QErrorMessage(self).showMessage("Connection failed")
            return
        # fill the table with the data
        self.data_table.setRowCount(len(data))
        self.data_table.setColumnCount(len(data[0]))
        self.data_table.setHorizontalHeaderLabels([i[0] for i in cursor.description])
        self.data_table.setHorizontalHeaderLabels([i[0] for i in cursor.description])
        for row in range(len(data)):
            self.connection_bar.setValue(20)
            for column in range(len(data[0])):
                self.data_table.setItem(row, column, QTableWidgetItem(str(data[row][column])))
            self.connection_bar.setValue(100)
        self.connection_bar.setValue(0)

    def generate_row_area(self):
        if self.db_data is None:
            return
        # remove current rows
        for i in reversed(range(self.row_layout.rowCount())):
            self.row_layout.removeRow(i)
        # get table and its columns
        table = self.table_input.currentText()
        try:
            with closing(mysql.connector.connect(**self.db_data)) as db:
                self.connection_bar.setValue(100)
                cursor = db.cursor()
                cursor.execute("SHOW columns FROM %s" % table)
                cols = cursor.fetchall()
                cursor.close()
        except:
            # show error and return
            QErrorMessage(self).showMessage("Connection failed")
            return
        # generate labels and lineedits for all columns
        for column in cols:
            label = QLabel(column[0])
            input_field = QLineEdit()
            input_field.setMinimumHeight(22)
            input_field.returnPressed.connect(self.insert_button.click)
            self.row_layout.addRow(label, input_field)
        self.connection_bar.setValue(0)

    def insert_data(self):
        succ_counter = 0
        table_name = self.table_input.currentText()
        vals = []
        rows = []
        data = []
        user_data = []
        keywords = ["gibberish", "rand_number"]
        # create db connection
        for i in range(self.row_layout.rowCount()):
            if self.row_layout.itemAt(i, QFormLayout.FieldRole).widget().displayText() != "":
                rows.append(self.row_layout.itemAt(i, QFormLayout.LabelRole).widget().text())
                vals.append(self.row_layout.itemAt(i, QFormLayout.FieldRole).widget().displayText())
        self.start_worker_thread(table_name, vals, rows, self.amount_input.value())
        self.disable_inserts(True)
        return


    def start_worker_thread(self, table_name, vals, rows, amount):
        self.connection_bar.setValue(0)
        self.connection_bar.setMaximum(amount)
        self.worker_thread = InsertWorkerThread()
        self.worker_thread.set_data(self.db_data, table_name, vals, rows, amount)
        self.worker_thread.inserts_done.connect(self.worker_thread_finished)
        self.worker_thread.current_progress.connect(self.update_progress_bar)
        self.worker_thread.start()
    @Slot (int)
    def update_progress_bar(self, p):
        self.connection_bar.setValue(p)

    @Slot (int)
    def worker_thread_finished(self, amount):
        self.disable_inserts(False)
        msg = QMessageBox(self)
        msg.setText("%d successful insert(s)" % amount)
        msg.open()

    def disable_inserts(self, bool):
        self.insert_button.setDisabled(bool)
        self.amount_input.setDisabled(bool)
        self.table_input.setDisabled(bool)
    def insert_factory(self, table_name, vals, rows, data=None, user_data=None):
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


class DBInputDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("DBInputDialog")
        self.outer_layout = QVBoxLayout()
        self.layout = QFormLayout()
        self.outer_layout.addLayout(self.layout)
        self.setLayout(self.outer_layout)
        self.username_label = QLabel("user")
        self.username_input = QLineEdit()
        self.layout.addRow(self.username_label, self.username_input)
        self.password_label = QLabel("password")
        self.password_input = QLineEdit()
        self.layout.addRow(self.password_label, self.password_input)
        self.database_label = QLabel("database")
        self.database_input = QLineEdit()
        self.layout.addRow(self.database_label, self.database_input)
        self.host_label = QLabel("host")
        self.host_input = QLineEdit("127.0.0.1")
        self.layout.addRow(self.host_label, self.host_input)
        self.port_label = QLabel("port")
        self.port_input = QLineEdit("3306")
        self.layout.addRow(self.port_label, self.port_input)
        self.button_layout = QHBoxLayout(self)
        self.confirm_button = QPushButton("Connect")
        self.confirm_button.clicked.connect(self.accept)
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.reject)
        self.button_layout.addWidget(self.confirm_button)
        self.button_layout.addWidget(self.cancel_button)
        self.outer_layout.addLayout(self.button_layout)

    def get_data(parent):
        # create and execute a dialog
        dialog = DBInputDialog(parent)
        dialog.exec_()
        # return the result
        return dialog.result(), dialog.return_data()

    def return_data(self):
        data = {}
        # build dict with labeltext as key and lineedit text as value
        for i in range(self.layout.rowCount()):
            data[self.layout.itemAt(i, QFormLayout.LabelRole).widget().text()] = self.layout.itemAt(i,
                                                                                                    QFormLayout.FieldRole).widget().displayText()
        return data




if __name__ == "__main__":
    app = QApplication([])
    app.setStyle(QStyleFactory.create("Fusion"))
    widget = DBFillerWidget()
    widget.show()
    widget.connect_to_database()
    sys.exit(app.exec_())
