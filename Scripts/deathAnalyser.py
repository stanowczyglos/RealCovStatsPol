#!/usr/bin/python3
# -*- coding: utf-8 -*-

import csv
import os
from datetime import datetime, timedelta
import pandas as pd
from zipfile import ZipFile
import urllib
import configparser
import itertools
import logging

#data sources to be downloaded
polUrl = 'https://arcgis.com/sharing/rest/content/items/a8c562ead9c54e13a135b02e0d875ffb/data'
uscURL = 'https://api.dane.gov.pl/media/resources/20211228/Liczba_zgon%C3%B3w_w_Rejestrze_Stanu_Cywilnego_w_latach_2015-2021_-_dane_tygodniowe_og%C3%B3%C5%82em.csv'
deUrl = 'https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Fallzahlen_Gesamtuebersicht.xlsx?__blob=publicationFile'
owidUrl = 'https://covid.ourworldindata.org/data/owid-covid-data.csv'

filePathPol = os.path.join("..", "OfficialDataSrc", "Poland", "DeathData", "arch.zip")
filePathPolUsc = os.path.join("..", "OfficialDataSrc", "Poland", "allDeathsWeekly.csv")
filePathDe = os.path.join("..", "OfficialDataSrc", "Germany_RKI", "Fallzahlen_Gesamtuebersicht.xlsx")
filePathDeCsv = os.path.join("..", "OfficialDataSrc", "Germany_RKI", "Fallzahlen_Gesamtuebersicht.csv")
filePathOwid = os.path.join("..", "OfficialDataSrc", "OWID", "owid-covid-data.csv")

filePath3Months = os.path.join("..", "calculatedData", "3months.csv")
filePathSinceBeginning = os.path.join("..", "calculatedData", "sinceBeginning.csv")
filePathAllDeathWeek = os.path.join("..", "calculatedData", "allDeathComparisonWeekly.csv")
filePathAllDeathCum = os.path.join("..", "calculatedData", "allDeathComparisonCumulative.csv")

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
        
        logging.info('Downloading data from polish registry office: ' + uscURL)
        req = urllib.request.Request(uscURL, headers={'User-Agent' : "Magic Browser"}) 
        con = urllib.request.urlopen(req)
        with open(filePathPolUsc, 'wb') as file:
            file.write(con.read())
        
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
        
    def createCsv(self, startDate, dateList, covDeathDate):
        countryList = []
        owidCountryList = []
        for elem in self.dict.keys():
            if elem == 'Poland':
                pol = PolishDeathData()
                logging.info('Parsing Poland data')
                pol.parser()
                pol.createFullCsv()
                pol.createAllDeathCsv(dateList, covDeathDate)
                countryList.append(pol.getDeathsFromDate(startDate))
            elif elem == 'Germany':
                de = GenericDeathData(0, -1, 4, filePathDeCsv, countryFilterList = [elem])
                logging.info('Parsing Germany data')
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
            df[self.dict[elem.name]] = pd.Series(elem.data).apply(str)
            
        with open(filePath3Months, 'w', newline='') as csvFile:
            logging.info('Create ' + filePath3Months)
            csvFile.write(df.to_csv(index = False))
    
class GenericCountryDeathData:
    def __init__(self, name, dataList = []):
        self.name = name
        self.data = dataList

class PolishDeathData:
    class DeathElem:
        def __init__(self, date, deathCov, deathCovMore, deathCovCumulative, deathCovMoreCumulative):
            self.date = datetime.strptime(date, '%Y%m%d') 
            self.cov = deathCov
            self.covMore = deathCovMore
            self.covCumulative = deathCovCumulative
            self.covMoreCumulative = deathCovMoreCumulative
            
    def __init__(self):
        self.deathList = []
        
    def parser(self):
        allDeathIdx = 0
        covDeathIdx = 0
        files = os.listdir(os.path.split(filePathPol)[0])
        files.sort()
        covCumulative = 0
        covMoreCumulative = 0
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
                    covCumulative += int(row[covDeathIdx])
                    covMoreCumulative += int(row[allDeathIdx])
                    self.deathList.append(self.DeathElem(filename.split('_')[0][:8],
                                          int(row[covDeathIdx]), int(row[allDeathIdx]), covCumulative, covMoreCumulative))

    def createFullCsv(self):
        #initial value taken from official Gov data for 23.11.2020
        allDeaths = 13774
        #below factor was calculated according to 2021 data. Deaths ratio between COV alone reason and COV with coexisting illnesses is 0.24
        covFactor = 0.24
        covDeaths = round(allDeaths * covFactor)
        
        outCsv = 'Data,Zgony COV+współistniejące,Zgony sam COV,Zgony COV+współistniejące narastająco,Zgony sam COV narastająco' + os.linesep
        for death in self.deathList:
            outCsv += str(death.date).split(' ')[0] + ',' + str(death.covMore) + ',' + str(death.cov) + ',' + \
                      str(death.covMoreCumulative + allDeaths) + ',' + str(death.covCumulative + covDeaths) + os.linesep
        with open(filePathSinceBeginning, newline='', mode='w') as csvFile:
            logging.info('Create: ' + filePathSinceBeginning)
            csvFile.write(outCsv)
    
    def createAllDeathCsv(self, yearList, covYear):
        csv = pd.read_csv(filePathPolUsc)
        csvOut = pd.DataFrame(columns=['Tydzień'])
        
        #lists to store weekly cov deaths
        covMoreList = [0 for i in range(datetime(int(covYear), 12, 31).isocalendar()[1])]
        covList = [0 for i in range(datetime(int(covYear), 12, 31).isocalendar()[1])]
        
        csvOut['Tydzień'] = csv['Nr tygodnia']
        for year in yearList:
            csvOut['Wszystkie zgony w ' + year] = csv[year]
            
        calcStart = False
        for elem in self.deathList:
            if True == calcStart:
                covMoreList[elem.date.isocalendar()[1]-1] += elem.covMore
                covList[elem.date.isocalendar()[1]-1] += elem.cov
            elif int(covYear) == elem.date.year and 1 == elem.date.isocalendar()[1]:
                calcStart = True
        
        csvOut['Zgony sam COV w ' + covYear] = pd.Series(covList)
        csvOut['Zgony COV + ch. współ. w ' + covYear] = pd.Series(covMoreList)
        
        csvOut.dropna(how='any', inplace=True)
        csvOut = csvOut.astype(int)
        logging.info('Create: ' + filePathAllDeathWeek)
        csvOut.to_csv(filePathAllDeathWeek, index=False)
        csvOut = csvOut.cumsum()
        csvOut['Tydzień'] = csv['Nr tygodnia']
        logging.info('Create: ' + filePathAllDeathCum)
        csvOut.to_csv(filePathAllDeathCum, index=False)
        
    def getDeathsFromDate(self, date):
        return GenericCountryDeathData("Poland", [death.cov for death in self.deathList if death.date >= date])

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

#class PolishAgeAnalyser:
    
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
        csvCreator.createCsv(datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(int(config['DATA_RANGE']['AllCountries'])),
                             config['DATA_RANGE']['AllDeathsYears'].split(','), config['DATA_RANGE']['AllDeathsCovYear'])
        dataCtx.cleanupData()

