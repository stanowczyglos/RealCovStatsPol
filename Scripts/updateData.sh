#!/bin/bash

#get RKI report and convert it to csv
cd ../OfficialDataSrc/Germany_RKI
wget https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Fallzahlen_Gesamtuebersicht.xlsx?__blob=publicationFile -O Fallzahlen_Gesamtuebersicht.xlsx
libreoffice --headless --convert-to csv Fallzahlen_Gesamtuebersicht.xlsx
#"csv:Text - txt - csv (StarCalc):59,34,76,1,1/1" Fallzahlen_Gesamtuebersicht.xlsx
rm Fallzahlen_Gesamtuebersicht.xlsx
cd -

#get OWID data
cd ../OfficialDataSrc/OWID
wget https://covid.ourworldindata.org/data/owid-covid-data.csv -O owid-covid-data.csv
#./owidExtractor.py
#rm owid-covid-data.csv
cd -

#get Polish data
cd ../OfficialDataSrc/Poland
wget https://arcgis.com/sharing/rest/content/items/a8c562ead9c54e13a135b02e0d875ffb/data -O DeathData/arch.zip
unzip -oqq DeathData/arch.zip -d DeathData
rm DeathData/arch.zip
#./MSZExtractor.py
wget https://basiw.mz.gov.pl/api/download/file?fileName=covid_pbi/zakaz_zgony_BKO/zgony.csv -O zgony.csv
wget https://basiw.mz.gov.pl/api/download/file?fileName=covid_pbi/zakaz_zgony_BKO/zakazenia.csv -O zakazenia.csv
./deathAnalyser.py
./positiveTestAnalyser.py
cd -

./deathAnalyser.py
#cleanup
rm ../OfficialDataSrc/OWID/owid-covid-data.csv
