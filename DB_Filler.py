import urllib.request
import json
from random import randint


def get_gibberish():
    url = "https://www.randomtext.me/api/gibberish/p-1/15-25"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:70.0) Gecko/20100101 Firefox/70.0"})
    x = urllib.request.urlopen(req)
    xdict = json.loads(x.read())
    return xdict["text_out"][3:-5]


def get_user_data():
    url = "https://randomuser.me/api"
    req = urllib.request.Request(url)
    data = urllib.request.urlopen(req)
    data_dict = json.loads(data.read())
    return data_dict["results"][0]


def insert_factory(table_name, values, rows=("")):
    return "INSERT INTO %s%s VALUES (%s)" % (table_name, "" if not rows else "(" + (",".join(rows)) + ")", ", ".join("\"%s\"" % (x) if type(x) is str else str(x) for x in values))


def get_rand_values(*args):
    output = []
    for arg in args:
        if callable(arg):
            output.append(arg())
        else:
            output.append(arg[randint(0, len(arg) - 1)])
    return output


print(get_user_data())