from DB_Filler import *
import random
sql = True
try:
    import mysql.connector
except:
    sql = False
amount = 5


def get_tweets(amount):
    output = []
    for i in range(0, amount):
        users = ["realDonaldTrump", "GERMama", "Goat", "Niels", "VladimirPutin", "SergioMozarella", "ukrainPresident"]
        if sql:
            try:
                db = mysql.connector.connect(host="localhost", user="root",database="twitter")
                cursor = db.cursor()
                cursor.execute("Select handle From Nutzer")
                result = cursor.fetchall()
                result = [x[0] for x in result]
                users = result
                cursor.close()
                db.close()
            except:
                print("Oof")
        output.append(insert_factory("Tweets", (get_rand_values(users, get_gibberish)), rows=("nutzerhandle", "text")) + ";")
    return output


def get_users(amount):
    output=[]
    for i in range(0, amount):
        user_data = get_user_data()
        handle = user_data["login"]["username"][0:15]
        display_name = " ".join(list(get_user_data()["name"].values())[1:])
        email = user_data["email"]
        verified = random.randint(0, 1)
        password = user_data["login"]["salt"]
        land_id = random.randint(1, 6)
        telnr = user_data["cell"]
        output.append(insert_factory("Nutzer", (handle, display_name, telnr, verified, password, land_id), rows=(
            "handle", "anzeigename", "telefonnummer", "verifiziert", "passwort", "land_id")) + ";")
    return output


def fill_database(data):
    if not sql:
        return
    try:
        db = mysql.connector.connect(host="localhost", user="root",database="twitter")
        cursor = db.cursor()
    except:
        print("OofoO")
        return
    if isinstance(data, list):
        for i in data:
            print(i)
            cursor.execute(i)
        db.commit()
    if isinstance(data, str):
        print(data)
        cursor.execute(data)
        db.commit()
    cursor.close()
    db.close()

if sql:
    try:
        db = mysql.connector.connect(host="localhost", user="root",database="twitter")
        cursor = db.cursor()
        cursor.execute("Select handle From Nutzer")
        result = cursor.fetchall()
        result = [x[0] for x in result]
        users = result
        cursor.execute("SELECT * FROM likes ORDER BY tweet_id")
        print(cursor.fetchall())
        cursor.close()
        db.close()
    except:
        print("Oof")
tweets = get_tweets(200)
"""""
for k in range(10, 41, 10):
    num = []
    for l in range(50):
        num.append(random.randint(43, 242))
        for i in range(k):
            try:
                fill_database(insert_factory("likes", (num[l], users[i]),("tweet_id", "nutzerhandle")))
            except:
                pass
                """