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
import re
from numpy import nan

#data sources to be downloaded
polDeathsUrl = 'https://arcgis.com/sharing/rest/content/items/a8c562ead9c54e13a135b02e0d875ffb/data'
polVaxUrl = 'https://arcgis.com/sharing/rest/content/items/b860f2797f7f4da789cb6fccf6bd5bc7/data'
polUscMetaDeathsUrl = 'https://api.dane.gov.pl/1.4/datasets/1953/resources/metadata.csv?lang=pl'
polUscMetaDeathsCovUrl = 'https://api.dane.gov.pl/1.4/datasets/2582/resources/metadata.csv?lang=pl'
polHospitalUrl = 'https://koronawirusunas.pl/u/polska-szpital'
polPopulationUrl = 'https://stat.gov.pl/podstawowe-dane/'

deUrl = 'https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Fallzahlen_Gesamtuebersicht.xlsx?__blob=publicationFile'
dePopulationUrl = 'https://www.destatis.de/EN/Themes/Society-Environment/Population/Current-Population/Tables/liste-current-population.html'
owidUrl = 'https://covid.ourworldindata.org/data/owid-covid-data.csv'
ecdcUrl = 'https://opendata.ecdc.europa.eu/covid19/vaccine_tracker/csv/data.csv'

filePathPolDeathZip = os.path.join("..", "OfficialDataSrc", "Poland", "DeathData", "arch.zip")
filePathPolInitialDeath = os.path.join("..", "OfficialDataSrc", "Poland", "DeathData", "Zakazenia30323112020.csv")
filePathPolVaxZip = os.path.join("..", "OfficialDataSrc", "Poland", "VaxData", "arch.zip")
filePathPolUsc = os.path.join("..", "OfficialDataSrc", "Poland", "allDeathsWeekly.csv")
filePathPolDeath = os.path.join("..", "OfficialDataSrc", "Poland", "zgony.csv")
filePathPolCases = os.path.join("..", "OfficialDataSrc", "Poland", "zakazenia.csv")
filePathDe = os.path.join("..", "OfficialDataSrc", "Germany_RKI", "Fallzahlen_Gesamtuebersicht.xlsx")
filePathDeCsv = os.path.join("..", "OfficialDataSrc", "Germany_RKI", "Fallzahlen_Gesamtuebersicht.csv")
filePathEcdc = os.path.join("..", "OfficialDataSrc", "ECDC", "ecdc-vax-data.csv")

filePath3Months = os.path.join("..", "calculatedData", "3months.csv")
filePathSinceBeginning = os.path.join("..", "calculatedData", "sinceBeginning.csv")
filePathcaseDeathIsolation = os.path.join("..", "calculatedData", "deathsCasesIsolationFromDate.csv")
filePathPolVax = os.path.join("..", "calculatedData", "polVax.csv")
filePathAllDeathWeek = os.path.join("..", "calculatedData", "allDeathComparisonWeekly.csv")
filePathAllDeathCum = os.path.join("..", "calculatedData", "allDeathComparisonCumulative.csv")
filePathPolDeathCalc = os.path.join("..", "calculatedData", "polDeathsCalc.csv")
filePathPolCasesCalc = os.path.join("..", "calculatedData", "polCasesCalc.csv")
filePathPolDeathWeeklyCalc = os.path.join("..", "calculatedData", "polDeathsWeeklyCalc.csv")
filePathPolDeathDailyCalc = os.path.join("..", "calculatedData", "polDeathsDaylyCalc.csv")
filePathPolCasesWeeklyCalc = os.path.join("..", "calculatedData", "polCasesWeeklyCalc.csv")
filePathPolCasesDailyCalc = os.path.join("..", "calculatedData", "polCasesDailyCalc.csv")
filePathPolHospital = os.path.join("..", "calculatedData", "hospitalBeds.csv")
filePathEcdcVaxCalc = os.path.join("..", "calculatedData", "vaxWeeklyCalc.csv")

def convDate2WeekSum(dateList, dataList):
    if len(dateList) != len(dataList):
        print("failure len")
        return
    
    #lists to store weekly cov deaths
    dateWeek = []
    data = []
    
    prevWeek = 0
    idx = -1
    for i in range(len(dateList)):
        week = dateList[i].strftime('%GW%V')
        if prevWeek != week:
            data.append(0)
            dateWeek.append(week)
            idx += 1
            
        data[idx] += dataList[i]
        prevWeek = week
    return dateWeek, data

    
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
    
    def downloadData(self, url, filePath = None, skipCheck = True):
        performDownload = True
        req = urllib.request.Request(url, headers={'User-Agent' : "Magic Browser"}) 
        site = urllib.request.urlopen(req)
        if False == skipCheck:
            meta = site.info()
            fileLen = meta.get("Content-Length")
            #download file only if it has different length than already existing file
            if True == os.path.exists(filePath) and os.stat(filePath).st_size == int(fileLen):
                performDownload = False
                logging.debug('Skipping download of ' + os.path.split(filePath)[1] + ' data from: ' + url)
                return True
                
        if True == performDownload:
            if None != filePath:
                logging.info('Downloading ' + os.path.split(filePath)[1] + ' data from: ' + url)
                with open(filePath, 'wb') as file:
                    file.write(site.read())
                    return True
            else:
                # req = urllib.request.Request(dePopulationUrl)
                # resp = urllib.request.urlopen(req)
                respData = site.read().decode('iso-8859-1')
                return respData
        return False

    def prepareData(self):
        #download and prepare polish data
        if True == self.downloadData(polDeathsUrl, filePathPolDeathZip, False):
            with ZipFile(filePathPolDeathZip, 'r') as zipObj:
                zipObj.extractall(os.path.split(filePathPolDeathZip)[0])
            
        if True == self.downloadData(polVaxUrl, filePathPolVaxZip, False):
            with ZipFile(filePathPolVaxZip, 'r') as zipObj:
                for file in zipObj.namelist():
                    if -1 != file.find('_rap_rcb_global_szczepienia.csv'):
                        zipObj.extract(file, os.path.split(filePathPolVaxZip)[0])
        
        #download COVID deaths and cases for age comparisons
        tmpdf = pd.read_csv(polUscMetaDeathsCovUrl, delimiter=';', encoding='iso-8859-1')
        tmpdf = tmpdf['URL pliku (do pobrania)'].sort_values(ascending=False)
        tmpLinkList = tmpdf.iloc[0:2].tolist()
        if -1 != tmpLinkList[0].find('zgony'):
            self.downloadData(tmpLinkList[0], filePathPolDeath, False)
            self.downloadData(tmpLinkList[1], filePathPolCases, False)
        else:
            self.downloadData(tmpLinkList[1], filePathPolDeath, False)
            self.downloadData(tmpLinkList[0], filePathPolCases, False)
        
        #download death data in Poland from Urząd Stanu Cywilnego
        tmpUrl = re.search('https://api.dane.gov.pl/resources/[0-9]+,liczba-zgonow-zarejestrowanych-w-rejestrze-stanu-cywilnego-w-okresie-od-1-wrzesnia-2015-r-dane-tygodniowe/', self.downloadData(polUscMetaDeathsUrl))
        self.downloadData(tmpUrl.group(0) + 'csv', filePathPolUsc, False)
        
        #download and prepare german data
        if True == self.downloadData(deUrl, filePathDe):
            xlsx = pd.ExcelFile(filePathDe)
            cols = ['Anzahl COVID-19-Fälle', 'Differenz Vortag Fälle', 'Todesfälle', 'Differenz Vortag Todesfälle', 'Fälle ohne Todesfälle']
        
            df = xlsx.parse(header=2)
            for col in cols:
                df[col] = df[col].fillna(value=0).apply(self.parseFloatToIntCell)
            df['Fall-Verstorbenen-Anteil'] = df['Fall-Verstorbenen-Anteil'].fillna(value=0).apply(self.parsePercentCell)
            df.to_csv(filePathDeCsv, encoding='utf-8', index=False)
        
        logging.debug('Download complete')
        return True
    
    def cleanupData(self):
        logging.debug('Remove: ' + filePathPolDeathZip)
        os.remove(filePathPolDeathZip)
        logging.debug('Remove: ' + filePathPolVaxZip)
        os.remove(filePathPolVaxZip)
        logging.debug('Remove: ' + filePathDe)
        os.remove(filePathDe)
        
class GenericCsvDeathCreator:
    def __init__(self, countries, columns):
        dictInput = zip(countries, columns)
        self.dict = dict(dictInput)
        
    def createCsv(self, startDate, startDatePol, dateList, covDeathDateList):
        countryList = []
        owidCountryList = []
        for elem in self.dict.keys():
            if -1 != elem.find('Poland'):
                pol = PolishDeathData()
                logging.debug('Parsing Poland data')
                pol.parser(startDatePol)
                pol.createAllStatsCsv(dateList, covDeathDateList)
                tst = pol.getDeathsFromDate(startDate)
                countryList.extend(pol.getDeathsFromDate(startDate))
            elif elem == 'Germany':
                req = urllib.request.Request(dePopulationUrl)
                resp = urllib.request.urlopen(req)
                respData = resp.read().decode(resp.headers.get_content_charset())
                pattern = re.search('([0-9,]+)</td></tr><tr><td class="Vorspalte-ind1" colspan="1" rowspan="1">Male', str(respData))
                population = int(pattern.groups()[0].replace(',', ''))
                de = GenericDeathData(['Berichtsdatum', 'Differenz Vortag Todesfälle'], filePathDeCsv, [elem], population)
                logging.debug('Parsing Germany data')
                de.parser(startDate)
                countryList.extend(de.getDeaths())
            else:
                #Collect all OWID related countries to parse OWID file only once
                owidCountryList.append(elem)
        if owidCountryList != []:
            owid = GenericDeathData(['date', 'new_deaths_per_million', 'location'], owidUrl, owidCountryList)
            logging.debug('Parsing ' + str(owidCountryList) + ' data')
            owid.parser(startDate)
            countryList.extend(owid.getDeaths())
        
        date_range = pd.date_range(start=startDate.strftime('%Y-%m-%d'), end=datetime.utcnow(), freq='d')
        df = pd.DataFrame()
        df['Data'] = date_range
        for elem in countryList:
            df[self.dict[elem.name]] = pd.Series(elem.data)
            
        with open(filePath3Months, 'w', newline='') as csvFile:
            logging.debug('Create ' + filePath3Months)
            csvFile.write(df.to_csv(index = False, float_format='%.3f'))
    
class GenericCountryDeathData:
    def __init__(self, name, dataList = []):
        self.name = name
        self.data = dataList

class PolishDeathData:
    csvOut = []
    population = 0
            
    class AgeElem:
        def __init__(self, ageThreshold, useAllCalc, phrase):
            self.ageThreshold = ageThreshold
            self.useAllCalc = useAllCalc
            self.category = self.DeathCategory(useAllCalc, phrase)
            
        class DeathCategory:
            def __init__(self, useAllCalc=True, phrase='(ch. współ.)'):
                self.vaxHalf = [0, 'Szczepieni 1/2']
                if True == useAllCalc:
                    self.vaxHalfMore = [0, 'Szczepieni 1/2 ' + phrase]
                self.boost = [0, 'Booster']
                if True == useAllCalc:
                    self.boostMore = [0, 'Booster ' + phrase]
                self.vax = [0, 'Szczepieni']
                if True == useAllCalc:
                    self.vaxMore = [0, 'Szczepieni ' + phrase]
                self.noVax = [0, 'Nieszczepieni']
                if True == useAllCalc:
                    self.noVaxMore = [0, 'Nieszczepieni ' + phrase]
        
    class ParserElement:
        def __init__(self, dirPath, sumRow, columns, initHandler, handler, startDate):
            self.dirPath = dirPath
            self.sumRow = sumRow
            self.columns = columns
            self.initHandler = initHandler
            self.handler = handler
            self.startDate = startDate
            
    def initDeaths(self,):
        self.csvOut = pd.DataFrame(columns=['Data', 'Zgony COV+współistniejące', 'Zgony sam COV', 'Przypadki', 'Osoby na kwarantannie',
                                            'Wykonane testy', 'Testy pozytywne', 'Ozdrowieńcy'])
        initCsv = pd.read_csv(filePathPolInitialDeath, delimiter=';', encoding='Windows-1250')
        self.csvOut['Data'] = initCsv['Data']
        self.csvOut['Zgony COV+współistniejące'] = initCsv['Zgony']
        self.csvOut['Zgony sam COV'] = pd.Series(initCsv['Zgony']*0.24).round(0)
        self.csvOut['Ozdrowieńcy'] = initCsv['Ozdrowieńcy (dzienna)']
        self.csvOut['Testy pozytywne'] = initCsv['Kwarantanna']
        
    def parserDeaths(self, data, isFinal, startDate=None, filter=None):
        if False == isFinal:
            data.extend([0] * (self.csvOut.columns.size - len(data)))
            self.csvOut.loc[len(self.csvOut)] = data
        else:
            #initial value taken from official Gov data for 23.11.2020
            allDeaths = 0#13774
            #below factor was calculated according to 2021 data. Deaths ratio between COV alone reason and COV with coexisting illnesses is 0.24
            covFactor = 1#0.24
            covDeaths = round(allDeaths * covFactor)
            self.csvOut['Zgony COV+współistniejące narastająco'] = self.csvOut['Zgony COV+współistniejące'].cumsum() + allDeaths
            self.csvOut['Zgony sam COV narastająco'] = self.csvOut['Zgony sam COV'].cumsum() + covDeaths
            
            tmpCsv = self.csvOut.loc[:, ['Data', 'Zgony COV+współistniejące', 'Zgony sam COV', 'Zgony COV+współistniejące narastająco', 'Zgony sam COV narastająco']]
            tmpCsv.to_csv(filePathSinceBeginning, index=False)
            
            casesInit = 0#876333# + 23911
            recoveredInit = 0#454717# + 169171
            #initial data of active cases taken from: https://twitter.com/synxchaosu/status/1432985070843879431
            activeCaseInit = 156596
            self.csvOut.set_index('Data', inplace=True)
            self.csvOut = self.csvOut.loc[startDate :, :].copy()
            self.csvOut['Zgony COV+współistniejące narastająco'] = self.csvOut['Zgony COV+współistniejące'].cumsum()
            self.csvOut['Zgony sam COV narastająco'] = self.csvOut['Zgony sam COV'].cumsum()
            self.csvOut['Przypadki narastająco'] = self.csvOut['Przypadki'].cumsum() + casesInit
            self.csvOut['Ozdrowieńcy narastająco'] = self.csvOut['Ozdrowieńcy'].cumsum() + recoveredInit
            self.csvOut['Aktywne przypadki'] = self.csvOut['Przypadki narastająco'] - self.csvOut['Zgony COV+współistniejące narastająco'] - self.csvOut['Ozdrowieńcy narastająco']
            self.csvOut['Aktywne przypadki'] += activeCaseInit
            self.csvOut['Kwarantanna + Izolacja'] = self.csvOut['Aktywne przypadki'] + self.csvOut['Osoby na kwarantannie']
            #self.csvOut['% pozytywnych testów'] = self.csvOut['Testy pozytywne'] / self.csvOut['Wykonane testy']
            self.csvOut.to_csv(filePathcaseDeathIsolation)
        
    def initVax(self,):
        self.csvOut = pd.DataFrame({'Data' : [0], '1 dawka narastajaco' : [0], '2 dawka narastajaco' : [0], 'Pelna dawka narastajaco' : [0], '3 dawka narastajaco' : [0], 'Przypominająca dawka narastajaco' : [0]})
    
    def parserVax(self, data, isFinal, startDate=None, filter=None):
        if False == isFinal:
            if 3 == len(data):
                data.extend([0, 0, 0])
                if filter[0] == 'liczba_szczepien_ogolem':
                    data[1] -= data[2]
            elif 4 == len(data):
                data.extend([0, 0])
                if filter[0] == 'liczba_szczepien_ogolem':
                    data[1] -= data[2]
                    
            self.csvOut.loc[len(self.csvOut)] = data
        else:
            self.csvOut['Pelna dawka narastajaco'] = self.csvOut[['2 dawka narastajaco', 'Pelna dawka narastajaco']].max(axis=1)
            self.csvOut['1 dawka'] = self.csvOut['1 dawka narastajaco'].diff(1)
            self.csvOut['2 dawka'] = self.csvOut['2 dawka narastajaco'].diff(1)
            self.csvOut['3 dawka'] = self.csvOut['3 dawka narastajaco'].diff(1)
            self.csvOut['Przypominająca dawka'] = self.csvOut['Przypominająca dawka narastajaco'].diff(1)
            self.csvOut['Osoby z niepełną dawką'] = self.csvOut['1 dawka narastajaco'] - self.csvOut[['2 dawka narastajaco', 'Pelna dawka narastajaco']].max(axis=1)
            self.csvOut['Osoby z pelną dawką'] = self.csvOut[['2 dawka narastajaco', 'Pelna dawka narastajaco']].max(axis=1) - self.csvOut['3 dawka narastajaco'] - self.csvOut['Przypominająca dawka narastajaco']
            self.csvOut['Osoby z boosterem'] = self.csvOut['3 dawka narastajaco'] + self.csvOut['Przypominająca dawka narastajaco']
            self.csvOut['Osoby Nieszczepione'] = self.population - (self.csvOut['Osoby z niepełną dawką'] + self.csvOut['Osoby z pelną dawką'] + self.csvOut['Osoby z boosterem'] )
            self.csvOut = self.csvOut.drop(labels=0, axis=0)
            self.csvOut.to_csv(filePathPolVax, float_format = '%.0f', index=False)
            
    def parser(self, startDate):
        req = urllib.request.Request(polPopulationUrl)
        resp = urllib.request.urlopen(req)
        respData = resp.read().decode(resp.headers.get_content_charset())
        pattern = re.search('<h5>Ludność w tys.&nbsp;</h5>.*\n.*\n\t<h2><a href="[a-z\/:.,0-4-]+"><span><strong>([0-9]+) *([0-9]*)', str(respData))
        self.population = int(pattern.groups()[0]+pattern.groups()[1])*1000

        parserList = [self.ParserElement(os.path.split(filePathPolDeathZip)[0], 0,
                                         ['zgony', 'zgony_w_wyniku_covid_bez_chorob_wspolistniejacych', ('liczba_przypadkow', 'liczba_wszystkich_zakazen'), 'liczba_osob_objetych_kwarantanna',
                                          'liczba_wykonanych_testow', 'liczba_testow_z_wynikiem_pozytywnym', ('liczba_ozdrowiencow','')],
                                         self.initDeaths, self.parserDeaths, startDate),
                      self.ParserElement(os.path.split(filePathPolVaxZip)[0], -1,
                                         [('dawka_1_suma', 'liczba_szczepien_ogolem'), 'dawka_2_ogolem', 'zaszczepieni_finalnie', 'dawka_3_suma', 'dawka_przypominajaca'],
                                         self.initVax, self.parserVax, None)]
        for elem in parserList:
            files = os.listdir(elem.dirPath)
            files.sort()
            checkColumns = any(isinstance(col, tuple) for col in elem.columns)
            elem.initHandler()
            for filename in files:
                if -1 != filename.find('rap_rcb_'):
                    filter = []
                    csvIn = pd.read_csv(os.path.join(elem.dirPath, filename), delimiter=';', encoding='iso-8859-1')
                    if True == checkColumns:
                        for col in elem.columns:
                            if tuple == type(col):
                                for tupCol in col:
                                    if tupCol in csvIn.columns:
                                        filter.append(tupCol)
                                        break
                            elif col in csvIn.columns:
                                filter.append(col)
                    else:
                        filter = elem.columns
                    dateStr = filename[:8]
                    
                    elem.handler([str(dateStr[:4]) + '-' + str(dateStr[4:6]) + '-' + str(dateStr[6:8])] + \
                                 csvIn.loc[csvIn.index[elem.sumRow], filter].values.tolist(), False, None, filter)
            
            elem.handler(None, True, elem.startDate)
    
    def createAllStatsCsv(self, yearList, covYearList):
        csvIn = pd.read_csv(filePathPolUsc)
        csvDeaths = pd.DataFrame(columns=['Tydzień'])
        
        #lists to store weekly cov deaths
        covSickList = []
        covList = []
        
        csvDeaths['Tydzień'] = csvIn['Nr tygodnia']
        yearList.sort()
        for year in yearList:
            csvDeaths['Wszystkie ' + year] = csvIn[year]

        csv = pd.read_csv(filePathSinceBeginning)
        csv['Data'] = pd.to_datetime(csv['Data'])
        for covYear in covYearList:
            date1 = datetime.strptime(covYear+'W1/1', '%GW%V/%w')
            date2 = datetime.strptime(covYear+'W'+str(datetime.strptime(covYear+'-12-31', '%Y-%m-%d').isocalendar()[1])+'/0', '%GW%V/%w')
            dateRange = (csv['Data'] >= date1) & (csv['Data'] <= date2)
            tmp = csv[dateRange].copy()
            #days = (tmp.loc[0,'Data'] - date1).days
            
            #tmp['Data'] = pd.concat([pd.date_range(date1, tmp.loc[0,'Data']), tmp['Data']])
            #tmp.loc[0, 'Data'] = pd.Series(pd.date_range(date1, tmp.loc[0,'Data']))
            #print(type(days))
            #covList.append()
            covList.append(convDate2WeekSum(tmp['Data'].tolist(), tmp['Zgony sam COV'].tolist())[1])
            covSickList.append(convDate2WeekSum(tmp['Data'].tolist(), tmp['Zgony COV+współistniejące'].tolist())[1])

        for yearIdx in range(len(covYearList)):
            csvDeaths['COV ' + covYearList[yearIdx]] = pd.Series(covList[yearIdx])
            csvDeaths['COV(ch. w.) ' + covYearList[yearIdx]] = pd.Series(covSickList[yearIdx])
            csvDeaths['COV(wszystkie) ' + covYearList[yearIdx]] = csvDeaths['COV ' + covYearList[yearIdx]] + csvDeaths['COV(ch. w.) ' + covYearList[yearIdx]]
        
        csvDeaths.dropna(how='any', inplace=True)
        csvDeaths = csvDeaths.astype(int)
        logging.debug('Create: ' + filePathAllDeathWeek)
        csvDeaths.to_csv(filePathAllDeathWeek, index=False)
        csvDeaths = csvDeaths.cumsum()
        csvDeaths['Tydzień'] = csvIn['Nr tygodnia']
        logging.debug('Create: ' + filePathAllDeathCum)
        csvDeaths.to_csv(filePathAllDeathCum, index=False)
    
    def createBASIWCsv(self, dataType, analyzerEn, analyzerDateRange, ageDateRange, ageRange):
        ageCol = 'wiek'
        ageCatCol = 'kat_wiek'
        doseCatCol = 'dawka_ost'
        isSickCol = 'czy_wspolistniejace'
        if 'cases' == dataType:
            dateCol = 'data_rap_zakazenia'
            caseNoCol = 'liczba_zaraportowanych_zakazonych'
            caseRepeatNoCol = 'numer_zarazenia'
            filePathR = filePathPolCases
            filePathW = filePathPolCasesCalc
            filePathWeekW = filePathPolCasesWeeklyCalc
            filePathDailyW = filePathPolCasesDailyCalc
            useAllCalc = False
            phrase = '(ponowne)'
        else:
            dateCol = 'data_rap_zgonu'
            caseNoCol = 'liczba_zaraportowanych_zgonow'
            caseRepeatNoCol = ''
            filePathR = filePathPolDeath
            filePathW = filePathPolDeathCalc
            filePathWeekW = filePathPolDeathWeeklyCalc
            filePathDailyW = filePathPolDeathDailyCalc
            useAllCalc = True
            phrase = '(ch. współ.)'
            
        maskAnd = ' and '
        maskOr = ' or '
        maskBooster = '(' + doseCatCol + '== "uzupełniająca"' + maskOr + doseCatCol + '== "przypominajaca"' + ')'
        maskVax = '(' + doseCatCol + '== "pelna_dawka"' + ')'
        maskHalfVax = '(' + doseCatCol + '== "jedna_dawka"' + ')'
        maskNoVax = '(' + doseCatCol + '== 0)'
        
        df = pd.read_csv(filePathR, delimiter=',', encoding='Windows-1250', dtype={"producent": str, "dawka_ost": str, "czy_wspolistniejace" : int})
        df[dateCol] = pd.to_datetime(df[dateCol])
        df = df.fillna(0)
        df.sort_values(by=[dateCol, ageCatCol, ageCol], inplace=True)
        if True == analyzerEn:
            listDay = []
            listNoVax = []
            listVax = []
            listOneTime = []
            listTwoTimes = []
            listThreeTimes = []
            listFourTimes = []
            listOneTimeVax = []
            listTwoTimesVax = []
            listThreeTimesVax = []
            listFourTimesVax = []
            currDate = analyzerDateRange[0]
            csvLines = pd.DataFrame()
            while currDate <= analyzerDateRange[1]:
                dateFilter = (df[dateCol] == currDate)
                df_tmp = df[dateFilter].copy()
                
#                if currDate >= datetime.strptime("2022-02-07", '%Y-%m-%d') and currDate <= datetime.strptime("2022-02-08", '%Y-%m-%d'):
#                    df_tmp.to_csv(dataType + str(currDate) + '.csv')
                noVax = df_tmp.query(maskNoVax)[caseNoCol].sum() + df_tmp.query(maskHalfVax)[caseNoCol].sum()
                vax = df_tmp.query(maskVax)[caseNoCol].sum() + df_tmp.query(maskBooster)[caseNoCol].sum()
                if 'numer_zarazenia' == caseRepeatNoCol:
                    repeated = df_tmp.query(caseRepeatNoCol + '== 1 and (' + maskNoVax + maskOr + maskHalfVax + ')')[caseNoCol].sum()
                    listOneTime.append(repeated)
                    repeated = df_tmp.query(caseRepeatNoCol + '== 2 and (' + maskNoVax + maskOr + maskHalfVax + ')')[caseNoCol].sum()
                    listTwoTimes.append(repeated)
                    repeated = df_tmp.query(caseRepeatNoCol + '== 3 and (' + maskNoVax + maskOr + maskHalfVax + ')')[caseNoCol].sum()
                    listThreeTimes.append(repeated)
                    repeated = df_tmp.query(caseRepeatNoCol + '== 4 and (' + maskNoVax + maskOr + maskHalfVax + ')')[caseNoCol].sum()
                    listFourTimes.append(repeated)
                    repeated = df_tmp.query(caseRepeatNoCol + '== 1 and (' + maskVax + maskOr + maskBooster + ')')[caseNoCol].sum()
                    listOneTimeVax.append(repeated)
                    repeated = df_tmp.query(caseRepeatNoCol + '== 2 and (' + maskVax + maskOr + maskBooster + ')')[caseNoCol].sum()
                    listTwoTimesVax.append(repeated)
                    repeated = df_tmp.query(caseRepeatNoCol + '== 3 and (' + maskVax + maskOr + maskBooster + ')')[caseNoCol].sum()
                    listThreeTimesVax.append(repeated)
                    repeated = df_tmp.query(caseRepeatNoCol + '== 4 and (' + maskVax + maskOr + maskBooster + ')')[caseNoCol].sum()
                    listFourTimesVax.append(repeated)
                listDay.append(currDate.strftime('%Y-%m-%d'))
                listNoVax.append(noVax)
                listVax.append(vax)
                
                currDate += timedelta(days=1)

            maxLen = len(listDay)
            csvLines['Data'] = listDay
            csvLines['Nieszczepiony'] = listNoVax
            csvLines['Szczepiony'] = listVax
            if 'numer_zarazenia' == caseRepeatNoCol:
                csvLines['Zakażeni 1 raz (n.sz.)'] = listOneTime
                csvLines['Zakażeni 2 razy (n.sz.)'] = listTwoTimes
                csvLines['Zakażeni 3 razy (n.sz.)'] = listThreeTimes
                csvLines['Zakażeni 4 razy (n.sz.)'] = listFourTimes
                csvLines['Zakażeni 1 raz (sz.)'] = listOneTimeVax
                csvLines['Zakażeni 2 razy (sz.)'] = listTwoTimesVax
                csvLines['Zakażeni 3 razy (sz.)'] = listThreeTimesVax
                csvLines['Zakażeni 4 razy (sz.)'] = listFourTimesVax
#                csvLines['Zakażeni 1 raz (n.sz.)'] -= csvLines['Zakażeni 2 razy (n.sz.)']
#                csvLines['Zakażeni 2 razy (n.sz.)'] -= csvLines['Zakażeni 3 razy (n.sz.)']
#                csvLines['Zakażeni 3 razy (n.sz.)'] -= csvLines['Zakażeni 4 razy (n.sz.)']
#                csvLines['Zakażeni 1 raz (sz.)'] -= csvLines['Zakażeni 2 razy (sz.)']
#                csvLines['Zakażeni 2 razy (sz.)'] -= csvLines['Zakażeni 3 razy (sz.)']
#                csvLines['Zakażeni 3 razy (sz.)'] -= csvLines['Zakażeni 4 razy (sz.)']
                csvLines['Zakażeni 1 raz (n.sz.)'] = csvLines['Zakażeni 1 raz (n.sz.)'].cumsum()
                csvLines['Zakażeni 2 razy (n.sz.)'] = csvLines['Zakażeni 2 razy (n.sz.)'].cumsum()
                csvLines['Zakażeni 3 razy (n.sz.)'] = csvLines['Zakażeni 3 razy (n.sz.)'].cumsum()
                csvLines['Zakażeni 4 razy (n.sz.)'] = csvLines['Zakażeni 4 razy (n.sz.)'].cumsum()
                csvLines['Zakażeni 1 raz (sz.)'] = csvLines['Zakażeni 1 raz (sz.)'].cumsum()
                csvLines['Zakażeni 2 razy (sz.)'] = csvLines['Zakażeni 2 razy (sz.)'].cumsum()
                csvLines['Zakażeni 3 razy (sz.)'] = csvLines['Zakażeni 3 razy (sz.)'].cumsum()
                csvLines['Zakażeni 4 razy (sz.)'] = csvLines['Zakażeni 4 razy (sz.)'].cumsum()
            csvLines.to_csv(filePathDailyW, index=False)
            
            csvLines['Data'] = pd.to_datetime(csvLines['Data'])
            listData, listNoVax = convDate2WeekSum(csvLines['Data'].tolist(), csvLines['Nieszczepiony'].tolist())
            listVax = convDate2WeekSum(csvLines['Data'].tolist(), csvLines['Szczepiony'].tolist())[1]
            csvLines = pd.DataFrame()
            csvLines['Data'] = pd.Series(listData)
            csvLines['Nieszczepiony'] = pd.Series(listNoVax)
            csvLines['Szczepiony'] = pd.Series(listVax)
            csvLines.to_csv(filePathWeekW, index=False)
        dataList = []
        for age in ageRange:
            dataList.append(self.AgeElem(age, True, phrase))
        dataList.append(self.AgeElem((-1,-1), True, phrase))
        dateRange = (df[dateCol] >= ageDateRange[0]) & (df[dateCol] <= ageDateRange[1])
        df = df[dateRange]
        for elem in dataList:
            maskAnd = ' and '
            maskOr = ' or '
            if -1 != elem.ageThreshold[1]:
                maskAge = '(' + ageCol + '>=' + str(elem.ageThreshold[0]) + maskAnd + ageCol + '<=' + str(elem.ageThreshold[1]) + maskAnd + ageCatCol + '!= "BD")'
            else:
                maskAge = '(' + ageCol + '== 0 and ' + ageCatCol +'== "BD")'
            maskGeneric = maskAge

            if '' == caseRepeatNoCol and True == elem.useAllCalc:
                maskGeneric = maskAge + maskAnd + '(' + isSickCol + '== 0)'
                
            elem.category.vaxHalf[0] = df.query(maskGeneric + maskAnd + maskHalfVax)[caseNoCol].sum()
            elem.category.boost[0] = df.query(maskGeneric + maskAnd + maskBooster)[caseNoCol].sum()
            elem.category.vax[0] = df.query(maskGeneric + maskAnd + maskVax)[caseNoCol].sum()
            elem.category.noVax[0] = df.query(maskGeneric + maskAnd + maskNoVax)[caseNoCol].sum()
            if '' != caseRepeatNoCol:
                maskGeneric = maskAge + maskAnd + '(numer_zarazenia > 1)'
                elem.category.vaxHalfMore[0] = df.query(maskGeneric + maskAnd + maskHalfVax)[caseNoCol].sum()
                elem.category.boostMore[0] = df.query(maskGeneric + maskAnd + maskBooster)[caseNoCol].sum()
                elem.category.vaxMore[0] = df.query(maskGeneric + maskAnd + maskVax)[caseNoCol].sum()
                elem.category.noVaxMore[0] = df.query(maskGeneric + maskAnd + maskNoVax)[caseNoCol].sum()
            elif True == elem.useAllCalc:
                maskGeneric = maskAge + maskAnd + '(' + isSickCol + '== 1)'
                elem.category.vaxHalfMore[0] = df.query(maskGeneric + maskAnd + maskHalfVax)[caseNoCol].sum()
                elem.category.boostMore[0] = df.query(maskGeneric + maskAnd + maskBooster)[caseNoCol].sum()
                elem.category.vaxMore[0] = df.query(maskGeneric + maskAnd + maskVax)[caseNoCol].sum()
                elem.category.noVaxMore[0] = df.query(maskGeneric + maskAnd + maskNoVax)[caseNoCol].sum()
        sumNotVaccinated = 0
        sumVaccinated = 0
        for elem in dataList:
            sumNotVaccinated += elem.category.noVax[0] + elem.category.vaxHalf[0]
            if True == elem.useAllCalc and '' == caseRepeatNoCol:
                sumNotVaccinated += elem.category.noVaxMore[0] + elem.category.vaxHalfMore[0]
            sumVaccinated += elem.category.vax[0] + elem.category.boost[0]
            if True == elem.useAllCalc and '' == caseRepeatNoCol:
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
            #if 'More' not in key or (dataType != 'cases' and 'More' in key):
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
        
    def createHospitalCsv(self):
        req = urllib.request.Request(polHospitalUrl)
        resp = urllib.request.urlopen(req)
        respData = resp.read()
        date, data = zip(*re.findall('\{arg: \"([0-9.]{10})\",p_szpital\: ([0-9]+),p_chorzy\: [0-9]+,\},', str(respData)))
        # All hospital beds in Poland on 2021-11-03 - https://bdl.stat.gov.pl/BDL/dane/podgrup/temat - Łóżka w szpitalach ogólnych - wskaźniki (P2454)
        allHospBed = [167567 for i in range(len(date))]
        #https://twitter.com/MZ_GOV_PL/status/1479739653452357632?s=20
        allCovBed = [30551 for i in range(len(date))]
        
        csv = pd.DataFrame(columns=['Data', 'Wszystkie łóżka w szpitalach ogólnych', 'Zarezerwowane łóżka COV', 'Zajęte łóżka COV'])
        csv['Data'] = date
        csv['Wszystkie łóżka w szpitalach ogólnych'] = allHospBed
        csv['Zarezerwowane łóżka COV'] = allCovBed
        csv['Zajęte łóżka COV'] = data
        
        logging.debug('Create ' + filePathPolHospital)
        logging.info('Reserved Covid beds: ' + str(int(allCovBed[0])/int(allHospBed[0])*100) + '%')
        logging.info('Occupied Covid beds: ' + str(int(data[-1])/int(allHospBed[0])*100) + '%')
        logging.info('Occupied/Reserved Covid beds: ' + str(int(data[-1])/int(allCovBed[0])*100) + '%')
        
        csv.to_csv(filePathPolHospital, index=False)

    def getDeathsFromDate(self, date):
        df = pd.read_csv(filePathSinceBeginning, index_col='Data')
        df = df.loc[str(date)[0:10]:, ['Zgony sam COV', 'Zgony COV+współistniejące']] 
        df[['Zgony sam COV', 'Zgony COV+współistniejące']] *= 1000000 / self.population
        df['Zgony COV+współistniejące'] -= df['Zgony sam COV']
        return [GenericCountryDeathData("Poland COV", df['Zgony sam COV'].cumsum().tolist()),
                GenericCountryDeathData("Poland COV+Sick", df['Zgony COV+współistniejące'].cumsum().tolist())]

class GenericDeathData:
    def __init__(self, columns, filePath, countryFilterList=None, population=None):
        self.columns = columns
        self.filePath = filePath
        self.countryFilterList = countryFilterList
        self.population = population
        self.countryData = []
    
    def parser(self, startDate):
        factor = 1
        csvIn = pd.read_csv(self.filePath, index_col=self.columns[0], encoding='utf-8')
        
        for country in self.countryFilterList:
            if len(self.columns) > 2:
                mask = '(' + self.columns[2] + ' == "' + country + '")'
                coto = csvIn.query(mask).copy()
            elif 0 != self.population:
                coto = csvIn.copy()
                factor =  1000000 / self.population
            coto = coto.loc[str(startDate)[0:10] :, self.columns[1]]*factor
            self.countryData.append(GenericCountryDeathData(country, coto.cumsum().tolist()))

    def getDeaths(self):
        return self.countryData
    
if __name__ == '__main__':
    #set max possible number of threads to be used for calculations
    os.environ['NUMEXPR_MAX_THREADS'] = str(os.cpu_count())
    config = configparser.ConfigParser()
    config.read('config.ini')
    logFormat = '%(asctime)s: %(message)s'
    logLvl = logging.WARNING
    if config['LOGGING']['EnableDebug'] == 'yes':
        logLvl = logging.DEBUG
    elif config['LOGGING']['EnableInfo'] == 'yes':
        logLvl = logging.INFO
    logging.basicConfig(format=logFormat, level=logLvl)
    logging.info('Running script for ' + config['COUNTRIES']['List'] + ' countries')
    dataCtx = DataHandler()
    if True == dataCtx.prepareData():
        PolishDeathData().createHospitalCsv()
        csvCreator = GenericCsvDeathCreator(config['COUNTRIES']['List'].split(','), config['COUNTRIES']['CsvHdr'].split(','))
        csvCreator.createCsv(datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(int(config['DATA_RANGE']['AllCountries'])),
                             config['DATA_RANGE']['StartDatePolStats'], config['DATA_RANGE']['AllDeathsYears'].split(','), config['DATA_RANGE']['AllDeathsCovYear'].split(','))

        PolishDeathData().createBASIWCsv('cases', config['BASIW_STAT']['PolCaseDeathAnalyze'] == 'yes',
                                         (datetime.strptime(config['BASIW_STAT']['PolCaseDeathStartDate'], '%Y-%m-%d'), datetime.strptime(config['BASIW_STAT']['PolCaseDeathEndDate'], '%Y-%m-%d')),
                                         (datetime.strptime(config['BASIW_STAT']['PolAgeStartDate'], '%Y-%m-%d'), datetime.strptime(config['BASIW_STAT']['PolAgeEndDate'], '%Y-%m-%d')),
                                         eval(config['BASIW_STAT']['PolAgerange']))
        PolishDeathData().createBASIWCsv('deaths', config['BASIW_STAT']['PolCaseDeathAnalyze'] == 'yes',
                                         (datetime.strptime(config['BASIW_STAT']['PolCaseDeathStartDate'], '%Y-%m-%d'), datetime.strptime(config['BASIW_STAT']['PolCaseDeathEndDate'], '%Y-%m-%d')),
                                         (datetime.strptime(config['BASIW_STAT']['PolAgeStartDate'], '%Y-%m-%d'), datetime.strptime(config['BASIW_STAT']['PolAgeEndDate'], '%Y-%m-%d')),
                                         eval(config['BASIW_STAT']['PolAgerange']))
    if config['CLEANUP']['DeleteTmpFiles'] == 'yes':
        dataCtx.cleanupData()
    logging.info('All jobs done')

