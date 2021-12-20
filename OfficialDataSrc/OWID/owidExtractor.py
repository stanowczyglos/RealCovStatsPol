#!/usr/bin/python3
# -*- coding: utf-8 -*-

import csv
import os, sys
from datetime import datetime, timedelta

#config
#modify below line to pick start date
startDate = datetime.strptime("2021-09-21", '%Y-%m-%d')
#modify below list to extract it from owid-covid-data.csv
countryFilter = ["Netherlands", "United Kingdom"]

if __name__ == '__main__':
    countryIdx = 2
    covDeathIdx = 8
    dateIdx = 3
    outCsvCountryOffsetIdx = 2;
    
    countryData = []
    countryFilter.sort();
    
    outCsv = "Data;" + ';'.join(str(country) for country in countryFilter) + os.linesep

    countryDataItem = []
    foundItem = False
    with open("owid-covid-data.csv", newline='', encoding='iso-8859-1') as csvFile:
        reader = csv.reader(csvFile, delimiter=',')
        for row in reader:
            if countryFilter.count(row[countryIdx]) > 0 :
                date = datetime.strptime(row[dateIdx], '%Y-%m-%d')
                if date >= startDate:
                    countryDataItem.append(row[covDeathIdx])
                    foundItem = True
            elif foundItem == True:
                foundItem = False
                countryData.append(countryDataItem)
                countryDataItem = []
                print

    for i in range(countryData[0].__len__()):
        outCsv += str(startDate.strftime('%Y-%m-%d')) + ';' + ';'.join(str(data[i].split('.')[0]) for data in countryData) + os.linesep
        startDate += timedelta(1)
        
    with open("countriesExtract.csv", 'w') as csvFile:
        csvFile.write(outCsv)
    
