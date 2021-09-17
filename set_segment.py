import sqlite3
import math
import statistics
import numpy as np
import pandas as pd
import scipy
from os import listdir
from os.path import isfile, join
import matplotlib.pyplot as plt
# from graph_tool.all import *
from copy import deepcopy
from heapq import heappush, heappop, heappushpop
import time
import cProfile
import gc
from numpy import *
import math
import matplotlib.pyplot as plt
from functools import total_ordering

global ftdb 
ftdb = './ft.sqlite3'

global db
db = sqlite3.connect(ftdb)
global cache
cache = dict()
global absent
absent = set([])
global setlist
setlist = []

def create_sets(set_location, size):
	global setlist
	inverted_index = dict()
	sets = dict()
	word_set = set([])
	print("listing files from dir: " + set_location)
	# files = np.array([f for f in listdir(set_location) if (isfile(join(set_location, f)))])
	files = np.array([f for f in listdir(set_location)])
	print("sets listed")
	if __debug__:
		print(files)
	total = len(files)
	cur = 1
	for f in files:
		# if __debug__:
		# 	print("doing: " + str(cur) + '/' + str(total))
		if ('fra' in f):
			cur += 1
			continue
		try:
			with open (set_location + f, mode="r", encoding="utf-8") as file:
				# f = f.rsplit('.', 1)[0]
				sets[f] = set([])
				setlist.append(f)
				for line in file:
					line = line.replace('\n', '')
					sets[f].add(line)
					if line in inverted_index:
						inverted_index[line].add(f)
					else:
						word_set.add(line)
						inverted_index[line] = set([])
						inverted_index[line].add(f)
		except UnicodeDecodeError:
			continue
		if len(sets) >= size:
			break
		cur += 1
	# rindex : inverted index : value -> list of set names
	# sets : set name -> values
	# word set: set of all values
	return inverted_index, sets, word_set

def segment(lake, size):
	start = time.time()
	rindex, sets, word_set = create_sets(lake, size)
	end = time.time()
	print("create_sets elapsed: " + str(end - start))
	print("----------------------")
	print("Total: " + str(len(sets)))
	print("Tokens: " + str(len(word_set)))
	set_10_1000 = dict()
	interval_10_1000 = 250
	for col in sets:
		if len(sets[col]) > 10 and len(sets[col]) < 1000:
			set_10_1000[col] = sets[col]
	full = len(set_10_1000)
	sample_dict = dict()
	j = 1
	for col in set_10_1000:
		i = math.floor((len(set_10_1000[col])) / interval_10_1000)
		if i not in sample_dict:
			sample_dict[i] = 0
		print(len(set_10_1000[col]))
		print(str(j)+"/"+str(full))
		filepath = "segmentfiles_70/" + str(i) + ".interval"
		with open(filepath, "a+") as f:
			if sample_dict[i] < 50:
				f.write(col)
				f.write("\n")
				sample_dict[i] += 1
		j+=1 

segment("./data_lake_70/", 100000000)
