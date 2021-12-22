#!/usr/bin/python3
# -*- coding: utf-8 -*-


import csv
import os, sys

if __name__ == '__main__':
    allDeathIdx = 0
    covDeathIdx = 0
    dateIdx = 0
    outCsv = "Data;Zgony;Zgony COV" + os.linesep
    files = os.listdir(".")
    files.sort()
    for filename in files:
        if True == filename.endswith('eksport.csv'):
            csvRow = filename.split('_')[0][:8]
            with open(filename, newline='', encoding='iso-8859-1') as csvFile:
                reader = csv.reader(csvFile, delimiter=';')
                row = next(reader)
                iter = 0
                for col in row:
                    if col == "zgony":
                        allDeathIdx = iter
                    elif -1 != col.find("bez_chorob_wspolistniejacych"):
                        covDeathIdx = iter
                    iter += 1
                row = next(reader)
                csvRow += ';' + str(row[allDeathIdx]) + ';' + str(row[covDeathIdx]) + os.linesep
                outCsv+= csvRow
    with open("poland.csv", newline='', encoding='iso-8859-1', mode='w') as csvFile:
        csvFile.write(outCsv)
    
    
