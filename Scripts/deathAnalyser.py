#!/usr/bin/python3
# -*- coding: utf-8 -*-

import csv
import os
from datetime import datetime, timedelta
import pandas as pd
from zipfile import ZipFile
from urllib.request import urlretrieve
import urllib
import configparser
import itertools
import logging

#data sources to be downloaded
polUrl = 'https://arcgis.com/sharing/rest/content/items/a8c562ead9c54e13a135b02e0d875ffb/data'
deUrl = 'https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Fallzahlen_Gesamtuebersicht.xlsx?__blob=publicationFile'
owidUrl = 'https://covid.ourworldindata.org/data/owid-covid-data.csv'

filePathPol = os.path.join("..", "OfficialDataSrc", "Poland", "DeathData", "arch.zip")
filePathDe = os.path.join("..", "OfficialDataSrc", "Germany_RKI", "Fallzahlen_Gesamtuebersicht.xlsx")
filePathDeCsv = os.path.join("..", "OfficialDataSrc", "Germany_RKI", "Fallzahlen_Gesamtuebersicht.csv")
filePathOwid = os.path.join("..", "OfficialDataSrc", "OWID", "owid-covid-data.csv")

filePath3Months = os.path.join("..", "calculatedData", "3months.csv")
filePathSinceBeginning = os.path.join("..", "calculatedData", "sinceBeginning.csv")

#Logger - empty stub for later usage as global logger
log = None

class DataHandler:    
    #this function will be used for downloading and converting all data. For now it's hard to handle RKI xlsx.
    def parsePercentCell(self, val):
        if 0 != val:
            return format(val*100, '.2f') + '%'
        return ''
    
    def parseFloatToIntCell(self, val):
        if 0 != val:
            return int(val)
        return ''
    
    def downloadData(self):
        #download and prepare polish data
        logging.info('Downloading polish data from: ' + polUrl)
        req = urllib.request.Request(polUrl, headers={'User-Agent' : "Magic Browser"}) 
        con = urllib.request.urlopen(req)
        with open(filePathPol, 'wb') as file:
            file.write(con.read())
        with ZipFile(filePathPol, 'r') as zipObj:
            zipObj.extractall(os.path.split(filePathPol)[0])
        
        #download and prepare german data
        logging.info('Downloading german data from: ' + deUrl)
        req = urllib.request.Request(deUrl, headers={'User-Agent' : "Magic Browser"}) 
        con = urllib.request.urlopen(req)
        with open(filePathDe, 'wb') as file:
            file.write(con.read())
        xlsx = pd.ExcelFile(filePathDe)
        cols = ['Anzahl COVID-19-Fälle', 'Differenz Vortag Fälle', 'Todesfälle', 'Differenz Vortag Todesfälle', 'Fälle ohne Todesfälle']
    
        df = xlsx.parse(header=2)
        for col in cols:
            df[col] = df[col].fillna(value=0).apply(self.parseFloatToIntCell)
        df['Fall-Verstorbenen-Anteil'] = df['Fall-Verstorbenen-Anteil'].fillna(value=0).apply(self.parsePercentCell)
        df.to_csv(filePathDeCsv, encoding='utf-8', index=False)
        
        #download and prepare OWID data
        logging.info('Downloading data for other countries from: ' + owidUrl)
        req = urllib.request.Request(owidUrl, headers={'User-Agent' : "Magic Browser"}) 
        con = urllib.request.urlopen(req)
        with open(filePathOwid, 'wb') as file:
            file.write(con.read())
        
        logging.info('Download complete')
        return True
    
    def cleanupData(self):
        logging.info('Remove: ' + filePathPol)
        os.remove(filePathPol)
        logging.info('Remove: ' + filePathOwid)
        os.remove(filePathOwid)
        logging.info('Remove: ' + filePathDe)
        os.remove(filePathDe)
        
class GenericCsvDeathCreator:
    def __init__(self, countries, columns):
        dictInput = zip(countries, columns)
        self.dict = dict(dictInput)
        
    def createCsv(self, startDate):
        countryList = []
        owidCountryList = []
        for elem in self.dict.keys():
            if elem == 'Poland':
                pol = PolishDeathData()
                logging.info('Parsing polish data')
                pol.parser()
                pol.createFullCsv()
                countryList.append(pol.getDeathsFromDate(startDate))
            elif elem == 'Germany':
                de = GenericDeathData(0, -1, 4, filePathDeCsv, countryFilterList = [elem])
                logging.info('Parsing german data')
                de.parser(startDate)
                countryList.extend(de.getDeaths())
            else:
                #Collect all OWID related countries to parse OWID file only once
                owidCountryList.append(elem)
        if owidCountryList != []:
            owid = GenericDeathData(3, 2, 8, filePathOwid, countryFilterList = owidCountryList)
            logging.info('Parsing ' + str(owidCountryList) + ' data')
            owid.parser(startDate)
            countryList.extend(owid.getDeaths())
        
        date_range = pd.date_range(start=startDate.strftime('%Y-%m-%d'), end=datetime.utcnow(), freq='d')
        df = pd.DataFrame()
        df['Data'] = date_range
        for elem in countryList:
            df[self.dict[elem.name]] = pd.Series(elem.data)
            
        with open(filePath3Months, 'w', newline='') as csvFile:
            logging.info('Create ' + filePath3Months)
            csvFile.write(df.to_csv(index = False))
    
class GenericCountryDeathData:
    def __init__(self, name, dataList = []):
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
        files = os.listdir(os.path.split(filePathPol)[0])
        files.sort()
        for filename in files:
            if True == filename.endswith('eksport.csv'):
                with open(os.path.join(os.path.split(filePathPol)[0],filename), newline='', encoding='iso-8859-1') as csvFile:
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
        #initial value taken from official Gov data for 23.11.2020
        allDeaths = 13774
        #below factor was calculated according to 2021 data. Deaths ratio between COV alone reason and COV with coexisting illnesses is 0.24
        covFactor = 0.24
        covDeaths = round(allDeaths * covFactor)
        
        outCsv = 'Data,Zgony COV+współistniejące,Zgony sam COV,Zgony COV+współistniejące narastająco,Zgony sam COV narastająco' + os.linesep
        for elem in self.deathList:
            allDeaths += int(elem.deathCovMore)
            covDeaths += int(elem.deathCov)
            outCsv += str(elem.date).split(' ')[0] + ',' + str(elem.deathCovMore) + ',' + str(elem.deathCov) + ',' + str(allDeaths) + ',' + str(covDeaths) + os.linesep
        with open(filePathSinceBeginning, newline='', mode='w') as csvFile:
            logging.info('Create: ' + filePathSinceBeginning)
            csvFile.write(outCsv)
    
    def getDeathsFromDate(self, date):
        list = [death.deathCov for death in self.deathList if death.date >= date]
        return GenericCountryDeathData("Poland", list)

class GenericDeathData:
    def __init__(self, dateCol, countryCol, deathCol, filePath, dateFormat = '%Y-%m-%d', countryFilterList = []):
        self.dateCol = dateCol
        self.countryCol = countryCol
        self.deathCol = deathCol
        self.filePath = filePath
        self.dateFormat = dateFormat
        self.countryFilterList = countryFilterList
        self.countryData = []
    
    def parser(self, startDate):
        countryDataItem = []
        currentCountry = ''
        foundItem = False
        with open(self.filePath, newline='', encoding='iso-8859-1') as csvFile:
            reader = csv.reader(csvFile, delimiter=',')
            #dummy read to skip header
            next(reader)
            for row in reader:
                #handler only for OWID data
                if -1 != self.countryCol and currentCountry != '' and currentCountry != row[self.countryCol]:
                    if foundItem == True:
                        foundItem = False
                        self.countryData.append(GenericCountryDeathData(currentCountry, countryDataItem))
                        countryDataItem = []
                        currentCountry = ''
                if -1 == self.countryCol or 0 < self.countryFilterList.count(row[self.countryCol]):
                    date = datetime.strptime(row[self.dateCol], self.dateFormat)
                    if currentCountry == '':
                        if -1 != self.countryCol:
                            currentCountry = row[self.countryCol]
                        else:
                            currentCountry = self.countryFilterList[0]
                    if date >= startDate:
                        countryDataItem.append(row[self.deathCol].split('.')[0])
                        foundItem = True
        if countryDataItem != []:
            self.countryData.append(GenericCountryDeathData(currentCountry, countryDataItem))

    def getDeaths(self):
        return self.countryData


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')
    logFormat = '%(asctime)s: %(message)s'
    logLvl = logging.WARNING
    if config['LOGGING']['EnableInfo'] == 'yes':
        logLvl = logging.INFO
    logging.basicConfig(format=logFormat, level=logLvl)
    logging.info('Running script for ' + config['COUNTRIES']['List'] + ' countries')
    dataCtx = DataHandler()
    if True == dataCtx.downloadData():
        csvCreator = GenericCsvDeathCreator(config['COUNTRIES']['List'].split(','), config['COUNTRIES']['CsvHdr'].split(','))
        csvCreator.createCsv(datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(int(config['DATA_RANGE']['AllCountries'])))
        dataCtx.cleanupData()

