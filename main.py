from DB_Filler import *
import random

users = ["realDonaldTrump", "GERMama", "Goat", "Niels", "VladimirPutin", "SergioMozarella", "ukrainPresident"]
amount = 5

for i in range(0, amount):
    print(insert_factory("Tweets", (get_rand_values(users, get_gibberish)), rows=("nutzerhandle", "text"))+";")

