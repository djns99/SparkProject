import random
fname = ["sally", "troy", "rob", "alex", "daniel", "felicity", "cameron", "john", "leroy", "jake", "boaty", "lemon", "cow", "david", "leon", "paul", "matthew", "trevor"]
lname = ["horse", "face", "McBoatFace", "stokes", "dally", "smith", "smithers", "jones", "kardashian", "bois", "keny", "wayne", "robertson"]
for i in range(0, 500):
	print(str(i) + "\t" + random.choice(fname) + "_" + random.choice(lname))
