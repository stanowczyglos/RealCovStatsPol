Projekt dotyczy głównie Polski, więc pierwsza wersja README będzie w moim ojczystym języku.

Niniejszy projekt ma na celu kolekcjonowanie publicznie dostępnych danych dotyczących zgonów i zakażeń koronawirusem SARS-COV2 i publikację ich w postaci grafik/wykresów. Nie ma on na celu nikogo do niczego przekonywać. Służy on publikacji suchych faktów w atrakcyjnej formie graficznej.

Źródła danych użytych do kalkulacji:
1. Dane zgonów w Polsce z podziałem na przyczynę zgonu (sam Covid vs Covid z chorobami współistniejącymi: https://arcgis.com/sharing/rest/content/items/a8c562ead9c54e13a135b02e0d875ffb/data (powyższy link do archiwum jest umieszczony na OFICJALNEJ stronie Ministerstwa Zdrowia: https://www.gov.pl/web/koronawirus/wykaz-zarazen-koronawirusem-sars-cov-2)
2. Dane zgonów na COV w Polsce z podziałem na rodzaj szczepienia i choroby współistniejące: https://basiw.mz.gov.pl/api/download/file?fileName=covid_pbi/zakaz_zgony_BKO/zgony.csv
3. Dane zakażeń COV w Polsce z podziałem na rodzaj szczepienia: https://basiw.mz.gov.pl/api/download/file?fileName=covid_pbi/zakaz_zgony_BKO/zakazenia.csv
4. Dane tygodniowe wszystkich zgonów w Polsce: https://api.dane.gov.pl/media/resources/20211228/Liczba_zgon%C3%B3w_w_Rejestrze_Stanu_Cywilnego_w_latach_2015-2021_-_dane_tygodniowe_og%C3%B3%C5%82em.csv
5. Dane zgonów z Niemiec (Instytut Roberta Kocha): https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Fallzahlen_Gesamtuebersicht.xlsx?__blob=publicationFile (nieniejszy plik jest aktualizowany przez Instytut od poniedziałku do piątku, w weekendy i święta, dane dostępne są na interaktywnej stronie aktualizowanej przez Instytut: https://experience.arcgis.com/experience/478220a4c454480e823b17327b2bf1d4/page/Landkreise/ - w przypadku rozbieżności między weekendowymi danymi, a dokumentem opublikowanym w poniedziałek, dane z tego drugiego nadpisują uprzednio umieszczone)
6. Dane z pozostałych krajów: https://covid.ourworldindata.org/data/owid-covid-data.csv – z tych danych korzysta google i wiele innych instutucji.

Wykresy:
Ze względu na łatwość obsługi i interaktywny charakter grafów wynikowych do publikacji wyników wybrałem platformę https://www.datawrapper.de/. Każdy utworzony wykres jest aktualizowany na bieżąco, a ich link jest stały, poszczególne Grafy są opisane w poniższych punktach:
1. Zgony dzienne COVID vs COVID współ. Polska od 24.11.2020 (https://datawrapper.dwcdn.net/F7sxY/) – Wykres ma za zadanie pokazywać ilość zgonów na samą chorobę COVID19, w stosunku do wszystkich zgłoszonych zgonów osób z pozytywnym wynikiem testu. Dane użyte do stworzenia wykresu pochodzą z linka [1].
2. Zgony dzienne COVID PL/DE/NL/UK 3msc (https://datawrapper.dwcdn.net/UiPSn/) - Prezentacja zgonów NA SAM COVID19 w Polsce (dane z linka[1]) w stosunku do innych krajów. Obecnie są to: Niemcy (dane z linka [4]), Holandia i Wielka Brytania (dane z linka [5]) w okresie 3 miesięcznym.
3. Zgony COVID w Polsce od września 2021 z podziałem na wiek z rozróżnieniem stanu zaszczepienia i typu zgonu (https://datawrapper.dwcdn.net/7IfMW/) - Źródłem danych jest link [2]
4. Zakażenia COVID w Polsce od września 2021 z podziałem na wiek z rozróżnieniem stanu zaszczepienia (https://datawrapper.dwcdn.net/6DraY/)

Aktualizacja danych:
Odbiór wszystkich danych i ich obróbka odbywa się poprzez uruchomienie skryptu: Scripts/deathAnalyser.py. Ściąga on wszystkie dane spod wypisanych wyżej linków i przetwarza otrzymane dane, celem użycia ich do wykresów.

Konfiguracja skryptu:
Skrypt deathAnalyser.py bazuje na pliku konfiguracyjnym Scripts/config.ini. Opis każdego pola znajduje się wewnątrz pliku konfiguracyjnego.

Metodologia kalkulacji:
1. Polska:
1.1. Zgony dzienne: Poszczególne pliki publikowane przez Ministerstwo Zdrowia są wypakowywane ze ściągniętego archiwum i umieszczane w repozytorium celem posiadania kopii zapasowej danych w przypadku wycofania się Ministerstwa z ich publikacji. Wszystkie pliki rozpakowywane są do katalogu: OfficialDataSrc/DeathData/Poland i tam przechowywane w repozytorium. Następnie skrypt analizuje wszystkie wypakowane pliki i kolekcjonuje dane z określonego w pliku konfiguracyjnym okresu. Wynikiem działania skryptu jest plik calculatedData/sinceBeginning.csv zawierający zgony na sam COV oraz na COV z chorobami współistniejącymi. Plik wynikowy jest importowany do datawrapper’a. Wszystkie zmiany i modyfikacje widoczne są w historii repozytorium.

1.2. Zgony COVID w zależności od wieku/zaszczepienia/chorób współistniejących – ściągnięty plik jest analizowany przez skrypt w określonym okresie czasu, a dane z pliku wynikowego: calculatedData/sinceBeginning.csv są importowane do wykresu [3]. Wszystkie zmiany i modyfikacje widoczne są w historii repozytorium.

1.3. Zakażenia COVID w zależności od wieku/zaszczepienia/– ściągnięty plik jest analizowany przez skrypt w określonym okresie czasu, a dane z pliku wynikowego: calculatedData/3months.csv są importowane do wykresu [4]. Wszystkie zmiany i modyfikacje widoczne są w historii repozytorium.

2. Niemcy:
Instytut im. Roberta Kocha (RKI) publikuje oficjalne statystyki dotyczące Niemiec. Zauważyłem, że dane w OWID w pewnych okresach były zawyżone i nie zgadzały się z danymi RKI. Podjąłem zatem decyzję o używaniu danych RKI. Zlokalizowane są one w katalogu OfficialDataSrc/Germany. Ponieważ instytut udostępnia plik xlsx, konieczna jest jego konwersja na CSV w trakcie wykonywania skryptu. Każda rewizja pliku OfficialDataSrc/Germany_RKI/Fallzahlen_Gesamtuebersicht.csv jest przechowywana w repozytorium, więc wszystkie zmiany można łatwo śledzić. Plik calculatedData/3months.csv jest importowany do wykresu [2]

3. Holandia, Wielka Brytania, inne:
https://ourworldindata.org/ - OWID - jest szanowanym i używanym m.in. przez google źródłem danych dotyczących koronawirusa. Udostępniony w linku [5] plik, zawiera dane z całego świata opóźnione o jeden dzień. Z uwagi na powyższe, dane z Polski i Niemiec brane są na bieżąco z oficjalnych danych, dane z pozostałych krajów - celem porównania historycznego – kolekcjonowane są z OWID. Początkowo miałem w planach przechowywać wszystkie zmiany w pliku OWID, niestety zmian było zbyt wiele i tak zaciemniały kontrolę historii. W związku z powyższym, skrypt odpowiada za pobranie danych OWID, ich analizuję pod kątem predefiniowanego okresu i krajów, a wynikowy plik: calculatedData/3months.csv, jest importowany do wykresu [2].
