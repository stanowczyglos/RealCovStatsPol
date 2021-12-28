#!/usr/bin/python3
# -*- coding: utf-8 -*-

import csv
import os
from datetime import datetime, timedelta
import pandas as pd
from zipfile import ZipFile
from urllib.request import urlretrieve
import urllib

#data sources to be downloaded
polUrl = 'https://arcgis.com/sharing/rest/content/items/a8c562ead9c54e13a135b02e0d875ffb/data'
deUrl = 'https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Fallzahlen_Gesamtuebersicht.xlsx?__blob=publicationFile'
owidUrl = 'https://covid.ourworldindata.org/data/owid-covid-data.csv'

#config
#modify below line to pick start date
startDate = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(90)
class GenericCountryDeathData:
    def __init__(self, name, dataList):
        self.name = name
        self.data = dataList

class PolishDeathData:
    class DeathElem:
        def __init__(self, date, deathCov, deathCovMore):
            self.date = datetime.strptime(date, '%Y%m%d') 
            self.deathCov = deathCov
            self.deathCovMore = deathCovMore
            
    def __init__(self):
        self.deathList = []
        
    def parser(self):
        allDeathIdx = 0
        covDeathIdx = 0
        dataPath = '../OfficialDataSrc/Poland/DeathData/'
        files = os.listdir(dataPath)
        files.sort()
        for filename in files:
            if True == filename.endswith('eksport.csv'):
                with open(dataPath + filename, newline='', encoding='iso-8859-1') as csvFile:
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
                    self.deathList.append(self.DeathElem(filename.split('_')[0][:8], row[covDeathIdx], row[allDeathIdx]))

    def createFullCsv(self):
        #display factor
        factor = 1
        #initial value taken from official Gov data for 23.11.2020
        allDeaths = 13774 * factor
        #below factor was calculated according to 2021 data. Deaths ratio between COV alone reason and COV with coexisting illnesses is 0.24
        covFactor = 0.24
        covDeaths = round(allDeaths * covFactor)
        
        outCsv = 'Data,Zgony COV+współistniejące,Zgony sam COV,Zgony COV+współistniejące narastająco,Zgony sam COV narastająco' + os.linesep
        for elem in self.deathList:
            allDeaths += int(elem.deathCovMore) * factor
            covDeaths += int(elem.deathCov) * factor
            outCsv += str(elem.date).split(' ')[0] + ',' + str(elem.deathCovMore) + ',' + str(elem.deathCov) + ',' + str(allDeaths) + ',' + str(covDeaths) + os.linesep
        with open('../calculatedData/sinceBeginning.csv', newline='', mode='w') as csvFile:
            csvFile.write(outCsv)
    
    def getDeathsFromDate(self, date):
        return GenericCountryDeathData("Poland", [death.deathCov for death in self.deathList if death.date >= date])

class GenericDeathData:
    def __init__(self, dateCol, countryCol, deathCol, filePath, dateFormat = '%Y-%m-%d', countryFilterList = []):
        self.dateCol = dateCol
        self.countryCol = countryCol
        self.deathCol = deathCol
        self.filePath = filePath
        self.dateFormat = dateFormat
        self.countryFilterList = countryFilterList
        self.countryData = []
    
    def parser(self):
        countryDataItem = []
        currentCountry = ''
        foundItem = False
        with open(self.filePath, newline='', encoding='iso-8859-1') as csvFile:
            reader = csv.reader(csvFile, delimiter=',')
            for row in reader:
                if -1 == self.countryCol or 0 < self.countryFilterList.count(row[self.countryCol]):
                    try:
                        date = datetime.strptime(row[self.dateCol], self.dateFormat)
                    except:
                        #print("Wrong date Format")
                        self.dateFormat
                    else:
                        if currentCountry == '':
                            if -1 != self.countryCol:
                                currentCountry = row[self.countryCol]
                            else:
                                currentCountry = self.countryFilterList[0]
                        if date >= startDate:
                            countryDataItem.append(row[self.deathCol].split('.')[0])
                            foundItem = True
                elif foundItem == True:
                    foundItem = False
                    self.countryData.append(GenericCountryDeathData(currentCountry, countryDataItem))
                    countryDataItem = []
                    currentCountry = ''
        if countryDataItem != []:
            self.countryData.append(GenericCountryDeathData(currentCountry, countryDataItem))

    def getDeaths(self):
        return self.countryData

#this function will be used for downloading and converting all data. For now it's hard to handle RKI xlsx.
def parseRkiPercent(val):
    if 0 != val:
        return format(val*100, '.2f') + '%'
    return ''

def parseRkiFloat(val):
    if 0 != val:
        return int(val)
    return ''

def downloadData():
    #download and prepare polish data
    path = '../OfficialDataSrc/Poland/DeathData/'
    filename = 'arch.zip'
    req = urllib.request.Request(polUrl, headers={'User-Agent' : "Magic Browser"}) 
    con = urllib.request.urlopen( req )
    with open(path + filename, 'wb') as file:
        file.write(con.read())
    with ZipFile(path + filename, 'r') as zipObj:
        zipObj.extractall(path)
    
    #download and prepare german data
    path = '../OfficialDataSrc/Germany_RKI/'
    filename = 'Fallzahlen_Gesamtuebersicht.xlsx'
    
    req = urllib.request.Request(deUrl, headers={'User-Agent' : "Magic Browser"}) 
    con = urllib.request.urlopen( req )
    with open(path + filename, 'wb') as file:
        file.write(con.read())
    xlsx = pd.ExcelFile(path + filename)
    cols = ['Anzahl COVID-19-Fälle', 'Differenz Vortag Fälle', 'Todesfälle', 'Differenz Vortag Todesfälle', 'Fälle ohne Todesfälle']

    df = xlsx.parse(header=2)
    for col in cols:
        df[col] = df[col].fillna(value=0).apply(parseRkiFloat)
    df['Fall-Verstorbenen-Anteil'] = df['Fall-Verstorbenen-Anteil'].fillna(value=0).apply(parseRkiPercent)
    df.to_csv(path + filename.split('.')[0] + '.csv', encoding='utf-8', index=False)
    
    #download and prepare OWID data
    path = '../OfficialDataSrc/OWID/'
    filename = 'owid-covid-data.csv'
    req = urllib.request.Request(owidUrl, headers={'User-Agent' : "Magic Browser"}) 
    con = urllib.request.urlopen( req )
    with open(path + filename, 'wb') as file:
        file.write(con.read())
    
    return True

def cleanupData():
    os.remove('../OfficialDataSrc/Poland/DeathData/arch.zip')
    os.remove('../OfficialDataSrc/OWID/owid-covid-data.csv')
    os.remove('../OfficialDataSrc/Germany_RKI/Fallzahlen_Gesamtuebersicht.xlsx')
    
if __name__ == '__main__':
    
    if True == downloadData():
        csvDict = {'Poland' : 'Polska zgony sam COV',
                   'Germany' : 'Niemcy zgony COV',
                   'Netherlands' : 'Holandia zgony COV',
                   'United Kingdom' : 'Wielka Brytania zgony COV'}
        countryList = []
        pol = PolishDeathData()
        owid = GenericDeathData(3, 2, 8, '../OfficialDataSrc/OWID/owid-covid-data.csv',
                                countryFilterList = ["Netherlands", "United Kingdom"])
        de = GenericDeathData(0, -1, 4, '../OfficialDataSrc/Germany_RKI/Fallzahlen_Gesamtuebersicht.csv',
                                '%m/%d/%Y', ['Germany'])
        pol.parser()
        pol.createFullCsv()
        de.parser()
        owid.parser()
        countryList.append(pol.getDeathsFromDate(startDate))
        countryList.extend(de.getDeaths())
        countryList.extend(owid.getDeaths())
        
        date_range = pd.date_range(start=startDate.strftime('%Y-%m-%d'), end=datetime.utcnow(), freq='d')
        df = pd.DataFrame()
        df['Data'] = date_range
        for elem in countryList:
            df[csvDict[elem.name]] = pd.Series(elem.data)
        
        with open("../calculatedData/3months.csv", 'w') as csvFile:
            csvFile.write(df.to_csv(index = False))
        cleanupData()

