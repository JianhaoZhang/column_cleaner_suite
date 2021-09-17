import pandas as pd
from os import listdir
from os.path import isfile, join
import os
import re

folder = "data_lake_70"
outfolder = "data_lake_70c"

print("Step 1: listing files...")
files = [f for f in listdir(folder) if isfile(join(folder, f))]

print(str(len(files)) + " files")
if 'progress.txt' in files:
	files.remove('progress.txt')

if not os.path.exists(outfolder):
    os.makedirs(outfolder)

print("Step 2: Reading history")
sets = set([])

if os.path.exists(outfolder + '/progress.txt'):
	with open(outfolder + '/progress.txt', 'r', encoding='utf-8') as f:
		sets = set(f.read().split(','))

print("Step 3: deduplicating")
i = 1

for file in files:
	progress = str(i) + '/' + str(len(files))
	if file in sets:
		print(progress + ' ' + file + ' is skipped')
		i += 1
		continue

	words = set(line.strip() for line in open(join(folder, file), 'r', encoding='utf-8'))
	splitwords = set([])
	k = 0
	for word in words:
		if ',' in word:
			for token in word.split(','):
				if bool(re.search(r'\d', token.strip())):
					continue
				splitwords.add(token.strip())
				k += 1
		else:
			try:
				float(word)
				continue
			except ValueError:
				if bool(re.search(r'\d', word)):
					continue
				else:
					splitwords.add(word)
					k += 1
	if k >= 10:
		with open(outfolder + '/' + file, 'w', encoding='utf-8') as f:
			for word in splitwords:
				f.write(str(word) + "\n")


	with open(outfolder + '/progress.txt', 'a', encoding='utf-8') as f:
			f.write(file + ',')
			print(progress + ' ' + file + ' is processed')
	i += 1
