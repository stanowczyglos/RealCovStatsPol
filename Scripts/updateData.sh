#!/bin/bash

#get Polish data
cd ../OfficialDataSrc/Poland
wget https://basiw.mz.gov.pl/api/download/file?fileName=covid_pbi/zakaz_zgony_BKO/zgony.csv -O zgony.csv
wget https://basiw.mz.gov.pl/api/download/file?fileName=covid_pbi/zakaz_zgony_BKO/zakazenia.csv -O zakazenia.csv
./deathAnalyser.py
./positiveTestAnalyser.py
cd -

#handle all other data
./deathAnalyser.py
