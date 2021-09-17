import pandas as pd
from os import listdir
from os.path import isfile, join
from io import StringIO
import os
import re
import sqlite3

ftdb = 'ft.sqlite3'
folder = "data_lake_deduplicated"
outfolder = "data_lake_70"

print("Step 1: listing files")
files = [f for f in listdir(folder) if isfile(join(folder, f))]

if 'progress.txt' in files:
	files.remove('progress.txt')

if not os.path.exists(outfolder):
    os.makedirs(outfolder)

print("Step 2: calculating progress")
sets = set([])

if os.path.exists(outfolder + '/progress.txt'):
	with open(outfolder + '/progress.txt', 'r', encoding='utf-8') as f:
		sets = set(f.read().split(','))

i = 1

print("Step 3: connecting to db")
dest = sqlite3.connect(ftdb)
tempfile = StringIO()
for line in dest.iterdump():
	tempfile.write('%s\n' % line)
dest.close()
tempfile.seek(0)

db = sqlite3.connect(':memory:')
db.cursor().executescript(tempfile.read())
db.commit()
db.row_factory = sqlite3.Row

cursor = db.cursor()
cursor.execute("SELECT word FROM wv")
ftwords = cursor.fetchall()
ftwords = set([w[0] for w in ftwords])
# print(ftwords)

print("Step 4: running")

for file in files:
	progress = str(i) + '/' + str(len(files))
	if file in sets:
		print(progress + ' ' + file + ' is skipped')
		i += 1
		continue

	words = set(line.strip().lower() for line in open(join(folder, file), 'r', encoding='utf-8'))

	k = 0
	for word in words:
		if word in ftwords:
			k += 1
	ratio = 0
	if len(words) > 0:
		ratio = float(k)/float(len(words))
	print(ratio)
	if ratio >= 0.7:
		with open(outfolder + '/' + file, 'w', encoding='utf-8') as f:
			for word in words:
				f.write(str(word) + "\n")


	with open(outfolder + '/progress.txt', 'a', encoding='utf-8') as f:
			f.write(file + ',')
			print(progress + ' ' + file + ' is processed')
	i += 1
