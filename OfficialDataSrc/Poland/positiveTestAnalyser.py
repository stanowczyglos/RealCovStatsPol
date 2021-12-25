#!/usr/bin/python3
# -*- coding: utf-8 -*-

import csv
import os, sys
from datetime import datetime, timedelta

class PatientDataClass:
    def __init__(self, ageThreshold):
        self.ageThreshold = ageThreshold
        self.category = self.PatientVaxCategory()
        
    class PatientVaxCategory:
        def __init__(self):
            self.vaxHalf = 0
            self.boost = 0
            self.vax = 0
            self.noVax = 0
            
        def __str__(self):
            return('vaxHalf: ' + str(self.vaxHalf) + '; boost: ' + str(self.boost) +
                   '; vax: ' + str(self.vax) + '; vaxMore: ' + '; noVax: ' + str(self.noVax))
        
#modify below line to pick start date
startDate = datetime.strptime("2021-09-01", '%Y-%m-%d')
endDate = datetime.strptime("2021-12-31", '%Y-%m-%d')

if __name__ == '__main__':
    dateIdx = 0
    ageIdx = 4
    doseIdx = 7
    illNoIdx= 9
    record=0
    dataList = [PatientDataClass(20), PatientDataClass(40), PatientDataClass(60), PatientDataClass(80), PatientDataClass(120), PatientDataClass(-1)]
    outCsv = "Stan," + ','.join(str(elem.ageThreshold) for elem in dataList) + os.linesep

    with open('zakazenia.csv', newline='', encoding='iso-8859-1') as csvFile:
        reader = csv.reader(csvFile, delimiter=';')
        next(reader)
        for row in reader:
            for elem in dataList:
                date = datetime.strptime(row[dateIdx], '%Y-%m-%d')
                if date >= startDate and date <= endDate:
                    lastDate = date
                    if row[ageIdx] == '':
                        if elem.ageThreshold == -1:
                            if row[doseIdx] == '':
                                elem.category.noVax += int(row[illNoIdx])
                            elif -1 != str('uzupelniajaca;przypominajaca').find(row[doseIdx]):
                                elem.category.boost += int(row[illNoIdx])
                            elif row[doseIdx] == 'pelna_dawka':
                                elem.category.vax += int(row[illNoIdx])
                            elif row[doseIdx] == 'jedna_dawka':
                                elem.category.vaxHalf+= int(row[illNoIdx])
                            else:
                                print(row)
                            record += int(row[illNoIdx])
                            break
                    elif int(row[ageIdx]) < elem.ageThreshold:
                        if row[doseIdx] == '':
                            elem.category.noVax += int(row[illNoIdx])
                        elif -1 != str('uzupelniajaca;przypominajaca').find(row[doseIdx]):
                            elem.category.boost += int(row[illNoIdx])
                        elif row[doseIdx] == 'pelna_dawka':
                            elem.category.vax += int(row[illNoIdx])
                        elif row[doseIdx] == 'jedna_dawka':
                            elem.category.vaxHalf+= int(row[illNoIdx])
                        else:
                            print(row)
                        record += int(row[illNoIdx])
                        break
    print("Number of patients " + str(record))

    deathCat = ['Szczepieni ½', 'Szczepieni ½ (ch. współ.)', 'Booster', 'Booster (ch. współ.)', 'Szczepieni',
                'Szczepieni (ch. współ.)','Nieszczepieni', 'Nieszczepieni (ch. współ.)']
    outCsv += 'Szczepieni 1/2,' + ','.join(str(elem.category.vaxHalf) for elem in dataList) + os.linesep
    outCsv += 'Booster,' + ','.join(str(elem.category.boost) for elem in dataList) + os.linesep
    outCsv += 'Szczepieni,' + ','.join(str(elem.category.vax) for elem in dataList) + os.linesep
    outCsv += 'Nieszczepieni,' + ','.join(str(elem.category.noVax) for elem in dataList) + os.linesep
    outCsv += "Dane z okresu: " + str(startDate) + " - " + str(lastDate) + os.linesep
        
    with open("zakazeniaCalc.csv", newline='', mode='w') as csvFile:
        csvFile.write(outCsv)
    
