#!/bin/bash

#get RKI report and convert it to csv
cd Germany_RKI
wget https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Fallzahlen_Gesamtuebersicht.xlsx?__blob=publicationFile -O Fallzahlen_Gesamtuebersicht.xlsx
libreoffice --headless --convert-to "csv:Text - txt - csv (StarCalc):59,34,76,1,1/1" Fallzahlen_Gesamtuebersicht.xlsx
rm Fallzahlen_Gesamtuebersicht.xlsx
cd ..

#get OWID data
cd OWID
wget https://covid.ourworldindata.org/data/owid-covid-data.csv -O owid-covid-data.csv
./owidExtractor.py
rm owid-covid-data.csv
cd ..

#get Polish data
cd Poland
wget https://arcgis.com/sharing/rest/content/items/a8c562ead9c54e13a135b02e0d875ffb/data -O arch.zip
unzip -oqq arch.zip
rm arch.zip
./MSZExtractor.py
cd ..
