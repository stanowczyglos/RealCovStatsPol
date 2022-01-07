#!/usr/bin/python3
# -*- coding: utf-8 -*-

import csv
import os
from datetime import datetime, timedelta
import pandas as pd
from zipfile import ZipFile
import urllib.request
import configparser
import logging

#data sources to be downloaded
polUrl = 'https://arcgis.com/sharing/rest/content/items/a8c562ead9c54e13a135b02e0d875ffb/data'
uscURL = 'https://api.dane.gov.pl/media/resources/20211228/Liczba_zgon%C3%B3w_w_Rejestrze_Stanu_Cywilnego_w_latach_2015-2021_-_dane_tygodniowe_og%C3%B3%C5%82em.csv'
basiwDeathUrl = 'https://basiw.mz.gov.pl/api/download/file?fileName=covid_pbi/zakaz_zgony_BKO/zgony.csv'
basiwCasesUrl = 'https://basiw.mz.gov.pl/api/download/file?fileName=covid_pbi/zakaz_zgony_BKO/zakazenia.csv'

deUrl = 'https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Fallzahlen_Gesamtuebersicht.xlsx?__blob=publicationFile'
owidUrl = 'https://covid.ourworldindata.org/data/owid-covid-data.csv'

filePathPol = os.path.join("..", "OfficialDataSrc", "Poland", "DeathData", "arch.zip")
filePathPolUsc = os.path.join("..", "OfficialDataSrc", "Poland", "allDeathsWeekly.csv")
filePathPolDeath = os.path.join("..", "OfficialDataSrc", "Poland", "zgony.csv")
filePathPolCases = os.path.join("..", "OfficialDataSrc", "Poland", "zakazenia.csv")
filePathDe = os.path.join("..", "OfficialDataSrc", "Germany_RKI", "Fallzahlen_Gesamtuebersicht.xlsx")
filePathDeCsv = os.path.join("..", "OfficialDataSrc", "Germany_RKI", "Fallzahlen_Gesamtuebersicht.csv")
filePathOwid = os.path.join("..", "OfficialDataSrc", "OWID", "owid-covid-data.csv")

filePath3Months = os.path.join("..", "calculatedData", "3months.csv")
filePathSinceBeginning = os.path.join("..", "calculatedData", "sinceBeginning.csv")
filePathAllDeathWeek = os.path.join("..", "calculatedData", "allDeathComparisonWeekly.csv")
filePathAllDeathCum = os.path.join("..", "calculatedData", "allDeathComparisonCumulative.csv")
filePathPolDeathCalc = os.path.join("..", "calculatedData", "polDeathsCalc.csv")
filePathPolCasesCalc = os.path.join("..", "calculatedData", "polCasesCalc.csv")

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
    
    def downloadData(self, url, filePath, skipCheck = True):
        performDownload = True
        req = urllib.request.Request(url, headers={'User-Agent' : "Magic Browser"}) 
        site = urllib.request.urlopen(req)
        if False == skipCheck:
            meta = site.info()
            fileLen = meta.get("Content-Length")
            #download file only if it has different length than already existing file
            if True == os.path.exists(filePath) and os.stat(filePath).st_size == int(fileLen):
                performDownload = False
                logging.info('Skipping download of ' + os.path.split(filePath)[1] + ' data from: ' + url)
                
        if True == performDownload:
            logging.info('Downloading ' + os.path.split(filePath)[1] + ' data from: ' + url)
            with open(filePath, 'wb') as file:
                file.write(site.read())
                return True
        return False

    def prepareData(self):
        #download and prepare polish data
        if True == self.downloadData(polUrl, filePathPol):
            with ZipFile(filePathPol, 'r') as zipObj:
                zipObj.extractall(os.path.split(filePathPol)[0])
        
        self.downloadData(basiwDeathUrl, filePathPolDeath, False)
        self.downloadData(basiwCasesUrl, filePathPolCases, False)
        
        #download and prepare german data
        if True == self.downloadData(deUrl, filePathDe):
            xlsx = pd.ExcelFile(filePathDe)
            cols = ['Anzahl COVID-19-Fälle', 'Differenz Vortag Fälle', 'Todesfälle', 'Differenz Vortag Todesfälle', 'Fälle ohne Todesfälle']
            
            df = xlsx.parse(header=2)
            for col in cols:
                df[col] = df[col].fillna(value=0).apply(self.parseFloatToIntCell)
            df['Fall-Verstorbenen-Anteil'] = df['Fall-Verstorbenen-Anteil'].fillna(value=0).apply(self.parsePercentCell)
            df.to_csv(filePathDeCsv, encoding='utf-8', index=False)
        
        #download and prepare OWID data
        self.downloadData(owidUrl, filePathOwid)
        
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
        
    def createCsv(self, startDate, dateList, covDeathDateList):
        countryList = []
        owidCountryList = []
        for elem in self.dict.keys():
            if elem == 'Poland':
                pol = PolishDeathData()
                logging.info('Parsing Poland data')
                pol.parser()
                pol.createFullCsv()
                pol.createAllDeathCsv(dateList, covDeathDateList)
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
            self.covSick = deathCovMore
            self.covCumulative = deathCovCumulative
            self.covMoreCumulative = deathCovMoreCumulative
            
    class AgeElem:
        def __init__(self, ageThreshold, useAllCalc=True):
            self.ageThreshold = ageThreshold
            self.useAllCalc = useAllCalc
            self.category = self.DeathCategory(useAllCalc)
            
        class DeathCategory:
            def __init__(self, useAllCalc=True):
                self.vaxHalf = [0, 'Szczepieni 1/2']
                if True == useAllCalc:
                    self.vaxHalfMore = [0, 'Szczepieni 1/2 (ch. współ.)']
                self.boost = [0, 'Booster']
                if True == useAllCalc:
                    self.boostMore = [0, 'Booster (ch. współ.)']
                self.vax = [0, 'Szczepieni']
                if True == useAllCalc:
                    self.vaxMore = [0, 'Szczepieni (ch. współ.)']
                self.noVax = [0, 'Nieszczepieni']
                if True == useAllCalc:
                    self.noVaxMore = [0, 'Nieszczepieni (ch. współ.)']
            
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
                    #it's faster to read just one line instead of opening each file with pandas
                    row = next(reader)
                    idx = 0
                    for col in row:
                        if col == "zgony":
                            allDeathIdx = idx
                        elif -1 != col.find("bez_chorob_wspolistniejacych"):
                            covDeathIdx = idx
                        idx += 1
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
            outCsv += str(death.date).split(' ')[0] + ',' + str(death.covSick) + ',' + str(death.cov) + ',' + \
                      str(death.covMoreCumulative + allDeaths) + ',' + str(death.covCumulative + covDeaths) + os.linesep
        with open(filePathSinceBeginning, newline='', mode='w', encoding='utf-8') as csvFile:
            logging.info('Create: ' + filePathSinceBeginning)
            csvFile.write(outCsv)
    
    def createAllDeathCsv(self, yearList, covYearList):
        csv = pd.read_csv(filePathPolUsc)
        csvOut = pd.DataFrame(columns=['Tydzień'])
        
        #lists to store weekly cov deaths
        covSickList = []
        covList = []
        for covYear in covYearList:
            covList.append([0 for i in range(datetime(int(covYear), 12, 31).isocalendar()[1])])
            covSickList.append([0 for i in range(datetime(int(covYear), 12, 31).isocalendar()[1])])
        
        csvOut['Tydzień'] = csv['Nr tygodnia']
        yearList.sort()
        for year in yearList:
            csvOut['Wszystkie zgony w ' + year] = csv[year]
        
        #check if start date's week is part of the year
        yearIdx = 0
        #find lowest calendar week available for lowest year
        prevElem = self.deathList[0]
        listIdx = -1
        for elem in self.deathList[1:]:
            if int(covYearList[yearIdx]) == elem.date.year:
                #check first week of proper year
                if 1 == elem.date.isocalendar()[1]:
                    listIdx = self.deathList.index(elem)
                    break
                #check lowest calendar week in proper year
                if self.deathList.index(prevElem) == 0 and prevElem.date.isocalendar()[1] <= elem.date.isocalendar()[1]:
                    listIdx = self.deathList.index(prevElem)
                    break
            prevElem = elem
        
        if -1 <= listIdx:
            for elem in self.deathList[listIdx:]:
                #check change of the year and calendar week
                if int(covYearList[yearIdx]) < elem.date.year and 1 == elem.date.isocalendar()[1]:
                    if yearIdx < len(covYearList) - 1:
                        yearIdx += 1
                    else:
                        logging.warning('createAllDeathCsv - year mismatch between COV data and input list')
                        break
                covSickList[yearIdx][elem.date.isocalendar()[1]-1] += elem.covSick
                covList[yearIdx][elem.date.isocalendar()[1]-1] += elem.cov
        
        for yearIdx in range(len(covYearList)):
            csvOut['Zgony sam COV w ' + covYearList[yearIdx]] = pd.Series(covList[yearIdx])
            csvOut['Zgony COV + ch. współ. w ' + covYearList[yearIdx]] = pd.Series(covSickList[yearIdx])
        
        csvOut.dropna(how='any', inplace=True)
        csvOut = csvOut.astype(int)
        logging.info('Create: ' + filePathAllDeathWeek)
        csvOut.to_csv(filePathAllDeathWeek, index=False)
        csvOut = csvOut.cumsum()
        csvOut['Tydzień'] = csv['Nr tygodnia']
        logging.info('Create: ' + filePathAllDeathCum)
        csvOut.to_csv(filePathAllDeathCum, index=False)
    
    def createBASIWCsv(self, dataType, startDate, endDate, ageRange):
        ageCol = 'wiek'
        ageCatCol = 'kat_wiek'
        doseCatCol = 'dawka_ost'
        isSickCol = 'czy_wspolistniejace'

        if 'cases' == dataType:
            dateCol = 'data_rap_zakazenia'
            caseNoCol = 'liczba_zaraportowanych_zakazonych'
            filePathR = filePathPolCases
            filePathW = filePathPolCasesCalc
            useAllCalc = False
        else:
            dateCol = 'data_rap_zgonu'
            caseNoCol = 'liczba_zaraportowanych_zgonow'
            filePathR = filePathPolDeath
            filePathW = filePathPolDeathCalc
            useAllCalc = True
            
        dataList = []
        for age in ageRange:
            dataList.append(self.AgeElem(age, useAllCalc))
        dataList.append(self.AgeElem((-1,-1), useAllCalc))
        
        df = pd.read_csv(filePathR, sep=';')
        df[dateCol] = pd.to_datetime(df[dateCol])
        dateRange = (df[dateCol] >= startDate) & (df[dateCol] <= endDate)
        df = df[dateRange]
        df.sort_values(by=[ageCatCol, ageCol], inplace=True)
        df = df.fillna(0)
        for elem in dataList:
            maskAnd = ' and '
            maskOr = ' or '
            if -1 != elem.ageThreshold[1]:
                maskAge = '(' + ageCol + '>=' + str(elem.ageThreshold[0]) + maskAnd + ageCol + '<=' + str(elem.ageThreshold[1]) + maskAnd + ageCatCol + '!= "BD")'
            else:
                maskAge = '(' + ageCol + '== 0 and ' + ageCatCol +'== "BD")'
            maskBooster = '(' + doseCatCol + '== "uzupelniajaca"' + maskOr + doseCatCol + '== "przypominajaca"' + ')'
            maskVax = '(' + doseCatCol + '== "pelna_dawka"' + ')'
            maskHalfVax = '(' + doseCatCol + '== "jedna_dawka"' + ')'
            maskNoVax = '(' + doseCatCol + '== 0)'
            maskGeneric = maskAge
            if True == elem.useAllCalc:
                maskGeneric = maskAge + maskAnd + '(' + isSickCol + '== "N")'
            
            elem.category.vaxHalf[0] = df.query(maskGeneric + maskAnd + maskHalfVax)[caseNoCol].sum()
            elem.category.boost[0] = df.query(maskGeneric + maskAnd + maskBooster)[caseNoCol].sum()
            elem.category.vax[0] = df.query(maskGeneric + maskAnd + maskVax)[caseNoCol].sum()
            elem.category.noVax[0] = df.query(maskGeneric + maskAnd + maskNoVax)[caseNoCol].sum()
            
            if True == elem.useAllCalc:
                maskGeneric = maskAge + maskAnd + '(' + isSickCol + '== "T")'
                elem.category.vaxHalfMore[0] = df.query(maskGeneric + maskAnd + maskHalfVax)[caseNoCol].sum()
                elem.category.boostMore[0] = df.query(maskGeneric + maskAnd + maskBooster)[caseNoCol].sum()
                elem.category.vaxMore[0] = df.query(maskGeneric + maskAnd + maskVax)[caseNoCol].sum()
                elem.category.noVaxMore[0] = df.query(maskGeneric + maskAnd + maskNoVax)[caseNoCol].sum()
        sumNotVaccinated = 0
        sumVaccinated = 0
        for elem in dataList:
            sumNotVaccinated += elem.category.noVax[0] + elem.category.vaxHalf[0]
            if True == elem.useAllCalc:
                sumNotVaccinated += elem.category.noVaxMore[0] + elem.category.vaxHalfMore[0]
            sumVaccinated += elem.category.vax[0] + elem.category.boost[0]
            if True == elem.useAllCalc:
                sumVaccinated += elem.category.vaxMore[0] + elem.category.boostMore[0]
        sumAll = sumNotVaccinated + sumVaccinated
        logging.info('Number of records in ' + filePathR + ': ' + str(sumAll))
        logging.info('Number of vaccinated ' + str(sumVaccinated) + '(' + str(sumVaccinated/sumAll * 100) + '%)')
        logging.info('Number of not vaccinated ' + str(sumNotVaccinated) + '(' + str(sumNotVaccinated/sumAll * 100) + '%)')
        
        colList = [str(str(elem.ageThreshold[0]) + '-' + str(elem.ageThreshold[1])) \
                       for elem in dataList if elem.ageThreshold[1] < 100 and elem.ageThreshold[0] >= 0]
        colList.extend([str(dataList[len(dataList)-2].ageThreshold[0]) + '+', 'Nieznany'])
        dict = vars(dataList[0].category)
        list = []
        for key in vars(dataList[0].category):
            if 'More' not in key or (dataType != 'cases' and 'More' in key):
                list.append(dict[key][1])
        iter = 0
        csvOut = pd.DataFrame()
        csvOut['Stan'] = pd.Series(list)
        
        for idx in range(len(dataList)):
            list = []
            dict = vars(dataList[idx].category)
            for key in dict:
                list.append(dict[key][0])
            csvOut[colList[idx]] = pd.Series(list)
        csvOut = csvOut.loc[:, (csvOut != 0).any(axis=0)]
        csvOut.to_csv(filePathW, index=False)
        
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
        with open(self.filePath, newline='', encoding='utf-8') as csvFile:
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
    if True == dataCtx.prepareData():
        csvCreator = GenericCsvDeathCreator(config['COUNTRIES']['List'].split(','), config['COUNTRIES']['CsvHdr'].split(','))
        csvCreator.createCsv(datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(int(config['DATA_RANGE']['AllCountries'])),
                             config['DATA_RANGE']['AllDeathsYears'].split(','), config['DATA_RANGE']['AllDeathsCovYear'].split(','))
        
        PolishDeathData().createBASIWCsv('cases', datetime.strptime(config['AGE_STAT']['PolAgeStartDate'], '%Y-%m-%d'),
                                       datetime.strptime(config['AGE_STAT']['PolAgeEndDate'], '%Y-%m-%d'), eval(config['AGE_STAT']['PolAgerange']))
        PolishDeathData().createBASIWCsv('deaths', datetime.strptime(config['AGE_STAT']['PolAgeStartDate'], '%Y-%m-%d'),
                                       datetime.strptime(config['AGE_STAT']['PolAgeEndDate'], '%Y-%m-%d'), eval(config['AGE_STAT']['PolAgerange']))
        dataCtx.cleanupData()

