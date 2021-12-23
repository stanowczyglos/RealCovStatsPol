#!/usr/bin/python3
# -*- coding: utf-8 -*-

import csv
import os, sys
from datetime import datetime, timedelta

class DeathDataClass:
    def __init__(self, ageThreshold):
        self.ageThreshold = ageThreshold
        self.category = self.DeathCategory()
        
    class DeathCategory:
        def __init__(self):
            self.vaxHalf = 0
            self.vaxHalfMore = 0
            self.boost = 0
            self.boostMore = 0
            self.vax = 0
            self.vaxMore = 0
            self.noVax = 0
            self.noVaxMore = 0
            
        def __str__(self):
            return('vaxHalf: ' + str(self.vaxHalf) + '; vaxHalfMore: ' + str(self.vaxHalfMore) + 
                  '; boost: ' + str(self.boost) + '; boostMore: ' + str(self.boostMore) + 
                  '; vax: ' + str(self.vax) + '; vaxMore: ' + str(self.vaxMore) + 
                  '; noVax: ' + str(self.noVax) + '; noVaxMore: ' + str(self.noVaxMore))
        
#modify below line to pick start date
startDate = datetime.strptime("2021-09-01", '%Y-%m-%d')
endDate = datetime.strptime("2021-12-31", '%Y-%m-%d')

if __name__ == '__main__':
    dateIdx = 0
    ageIdx = 4
    illIdx = 6
    doseIdx = 8
    deathNoIdx= 10
    record=0
    dataList = [DeathDataClass(20), DeathDataClass(40), DeathDataClass(60), DeathDataClass(80), DeathDataClass(120)]
    outCsv = "Stan;" + ';'.join(str(elem.ageThreshold) for elem in dataList) + os.linesep

    with open('zgony.csv', newline='', encoding='iso-8859-1') as csvFile:
        reader = csv.reader(csvFile, delimiter=';')
        next(reader)
        for row in reader:
            for elem in dataList:
                date = datetime.strptime(row[dateIdx], '%Y-%m-%d')
                if date >= startDate and date <= endDate:
                    lastDate = date
                    if int(row[ageIdx]) < elem.ageThreshold:
                        if row[doseIdx] == '':
                            if row[illIdx] == 'T':
                                elem.category.noVaxMore += int(row[deathNoIdx])
                            else:
                                elem.category.noVax += int(row[deathNoIdx])
                        elif -1 != str('uzupelniajaca;przypominajaca').find(row[doseIdx]):
                            if row[illIdx] == 'T':
                                elem.category.boostMore += int(row[deathNoIdx])
                            else:
                                elem.category.boost += int(row[deathNoIdx])
                        elif row[doseIdx] == 'pelna_dawka':
                            if row[illIdx] == 'T':
                                elem.category.vaxMore += int(row[deathNoIdx])
                            else:
                                elem.category.vax += int(row[deathNoIdx])
                        elif row[doseIdx] == 'jedna_dawka':
                            if row[illIdx] == 'T':
                                elem.category.vaxHalfMore += int(row[deathNoIdx])
                            else:
                                elem.category.vaxHalf+= int(row[deathNoIdx])
                        else:
                            print(row)
                        record += int(row[deathNoIdx])
                        break
    print("Number of deaths" + str(record))

    deathCat = ['Szczepieni ½', 'Szczepieni ½ (ch. współ.)', 'Booster', 'Booster (ch. współ.)', 'Szczepieni',
                'Szczepieni (ch. współ.)','Nieszczepieni', 'Nieszczepieni (ch. współ.)']
    outCsv += 'Szczepieni 1/2;' + ';'.join(str(elem.category.vaxHalf) for elem in dataList) + os.linesep
    outCsv += 'Szczepieni 1/2 (ch. współ.);' + ';'.join(str(elem.category.vaxHalfMore) for elem in dataList) + os.linesep
    outCsv += 'Booster;' + ';'.join(str(elem.category.boost) for elem in dataList) + os.linesep
    outCsv += 'Booster (ch. współ.);' + ';'.join(str(elem.category.boostMore) for elem in dataList) + os.linesep
    outCsv += 'Szczepieni;' + ';'.join(str(elem.category.vax) for elem in dataList) + os.linesep
    outCsv += 'Szczepieni (ch. współ.);' + ';'.join(str(elem.category.vaxMore) for elem in dataList) + os.linesep
    outCsv += 'Nieszczepieni;' + ';'.join(str(elem.category.noVax) for elem in dataList) + os.linesep
    outCsv += 'Nieszczepieni (ch. współ.);' + ';'.join(str(elem.category.noVaxMore) for elem in dataList) + os.linesep
    outCsv += "Dane z okresu: " + str(startDate) + " - " + str(lastDate) + os.linesep
        
    with open("zgonyCalc.csv", newline='', mode='w') as csvFile:
        csvFile.write(outCsv)
    
