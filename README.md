# Pomysł i cel biznesowy

Naszym celem biznesowym jest przeprowadzenie szczegółowej analizy danych zawartych w sprawozdaniach finansowych firm oraz zestawienie ich z informacjami dotyczącymi skarg, ich rodzajami oraz ilością. Nasze dane będą obejmować tylko firmy świadczące usługi finansowe, ze względu na charakterystykę zbioru ze zgłoszeniami do Consumer Financial Protection Bureau.

Chcemy zbadać te dane, aby zidentyfikować wzorce, trendy i zależności, które mogą pomóc w lepszym zrozumieniu relacji między kondycją finansową firm a zgłaszanymi skargami. Zaprezentowany przez nas system umożliwia zidentyfikowanie powodów składania skarg, miejsca ich występowania oraz zbadanie reakcji firmy na złożoną skargę. 

Za pomocą tych informacji firmy mogą wypracować strategię rozwiązywania konfliktów z klientami, a także zidentyfikować produkty, które są najbardziej problematyczne. Dodatkowo, stworzona przez nas hurtownia zawiera dane finansowe firm, co umożliwia identyfikację tych skarg, które mają największy wpływ na wyniki finansowe firm. 

Poprzez to połączenie danych dążymy do umożliwienia podejmowania trafniejszych decyzji biznesowych oraz skuteczniejszego reagowania na potrzeby i oczekiwania klientów analizowanych firm.

# Architektura

![image](https://github.com/user-attachments/assets/43193e22-c8b3-409b-8ba5-531133079f5f)

*Rysunek 1: Użyte technologie*

Podstawowym miejscem przechowywania danych jest baza danych PostgreSQL. Jako narzędzia do ETL użyliśmy Airflow, który będzie przetwarzał pliki `.csv` i `.txt`, a następnie ładował dane do naszej bazy danych. Power BI z kolei będzie nam służył do stworzenia interaktywnych raportów.

# Opis danych źródłowych

W projekcie użyłyśmy dwóch zbiorów danych:

- [Consumer Complaint Database](https://catalog.data.gov/dataset/consumer-complaint-database)  
  Zbiór skarg dotyczących produktów i usług finansowych dla konsumentów, które są wysyłane do firm w celu uzyskania odpowiedzi. Skargi są publikowane po odpowiedzi firmy, potwierdzeniu relacji handlowej z konsumentem lub po 15 dniach. Skargi przekazane innym regulatorom, na przykład dotyczące instytucji depozytowych z aktywami mniejszymi niż 10 miliardów dolarów, nie są publikowane. Baza danych jest zazwyczaj aktualizowana codziennie, dane są dostępne m.in. w formacie CSV.

- [Financial Statement Data Sets](https://catalog.data.gov/dataset/financial-statement-data-sets)  
  Zbiory danych zawierają informacje wyciągnięte z załączników EX-101 i obejmują kwartalne oraz roczne dane liczbowe z głównych sprawozdań finansowych. Dane są podzielone na cztery zbiory: SUB (dane o zgłoszeniach), NUM (dane liczbowe), TAG (informacje o tagach) i PRE (prezentacja danych w sprawozdaniach). Informacje są aktualizowane kwartalnie i dostępne w formacie TXT.


# Stworzona hurtownia danych

![image](https://github.com/user-attachments/assets/26ef1b71-bd57-4e39-8a01-bed79e9f8c66)

Jeśli kolumna jest wyspecyfikowana jako `NOT NULL` (każda w przypadku tej hurtowni) i nie ma podanej domyślnej wartości w poniższych tabelach, to będzie to `'NA'`.

## financial_statement_fact

| **Nazwa kolumny**       | **Typ**                     | **Skąd dane**                                | **Cel**                                             |
|-------------------------|-----------------------------|----------------------------------------------|-----------------------------------------------------|
| factitemid               | serial, not null            | własna                                       | Klucz główny tej tabeli                             |
| statementid              | numeric, not null           | Kolumna “adsh” z tabeli sub                  | Klucz obcy do tabeli                                |
| companyid                | numeric, not null           | własna, shashowane połączenie nazwy i startdate | Klucz obcy do tabeli CompanyDim                     |
| tagid                    | numeric, not null           | własna, shashowane połączenie tagu i wersji   | Klucz obcy do tabeli TagDim                         |
| companyname              | character varying(512), not null | Kolumna name z subs                          | Nazwa firmy                                         |
| coregistrantname          | character varying(512), not null | Na podstawie coreg z num                     | Coregistrant, dodana tutaj, ponieważ identyfikuje wartość |
| value                    | double precision, not null  | Kolumna value z tabeli nums                  | Wartość pieniężna                                   |
| unitofmeasure            | character varying(255), not null | Kolumna uom z tabeli nums                    | Rodzaj wartości np. waluta                         |
| enddateid                | bigint, not null            | Kolumna ddate z nums                         | Koniec okresu do którego ta wartość obowiązuje      |
| startdateid              | bigint, not null            | Obliczone z ddate i qtrs z tabeli nums       | Początek okresu ważności tej wartości               |


## statement_dim

| **Nazwa kolumny**        | **Typ**                     | **Skąd dane**                                | **Cel**                                             |
|--------------------------|-----------------------------|----------------------------------------------|-----------------------------------------------------|
| statementid               | numeric, not null           | Kolumna “adsh” z tabeli sub                  | Klucz główny tej tabeli                             |
| fiscalyear                | integer, null akceptowalne  | Kolumna fy z tabeli subs                     | Rok fiskalny                                        |
| fiscalperiod              | character varying(255), null akceptowalne | Kolumna fp z tabeli subs                   | Okres fiskalny                                      |
| formtype                  | character varying(255), not null | Kolumna form z tabeli subs                   | Rodzaj złożonego zeznania                           |
| balancesheetdateid        | bigint, not null            | Kolumna period z tabeli subs                 | Zwykle koniec okresu obejmującego zgłoszenie        |
| filleddateid              | bigint, not null            | Kolumna filed z tabeli subs                  | Kiedy zeznanie zostało złożone                      |
| accepteddateid            | bigint, not null            | Kolumna accepted z tabeli subs               | Kiedy zeznanie zostało zaakceptowane                |
| previousreport            | boolean, not null           | Kolumna prevrpt z tabeli subs                | Czy to zeznanie zostało poprawione                  |
| numbercik                 | numeric, not null           | Kolumna nicks z tabeli subs                  | Ilość firm, które obejmuje zeznanie                 |

## tags

| **Nazwa kolumny**         | **Typ**                         | **Skąd dane**                                | **Cel**                                             |
|---------------------------|---------------------------------|----------------------------------------------|-----------------------------------------------------|
| tagid                      | numeric, not null               | Własna, shashowane połączenie tagu i wersji  | Klucz główny                                        |
| tag                        | character varying(512), not null| Kolumna tag z tabeli tag                      | Nazwa tagu                                          |
| version                    | character varying(512), not null| Kolumna version z tabeli tag                  | Wersja tagu                                         |
| tagdescription             | text, not null                  | Kolumna tlabel z tabeli tag                   | Ładna nazwa tagu                                    |
| type                       | character varying(512), not null| Stworzone na podstawie kolumn datatype, crdr, abstract z tabeli tag | Określa, czy credit, debit, czy to nie jest wartość monetarna itd. |

## company_complaints_fact

| **Nazwa kolumny**         | **Typ**                         | **Skąd dane**                                | **Cel**                                             |
|---------------------------|---------------------------------|----------------------------------------------|-----------------------------------------------------|
| factid                     | serial                         | Własna, serial                               | Klucz główny tej tabeli                             |
| complaintId                | numeric, not null              | Complaint ID z complaints                    | Klucz obcy do tabeli complaints_dim                 |
| comapnyid                  | numeric, not null              | Własna                                      | Klucz obcy do tabeli company_dim                    |
| companyname                | varchar(512), not null         | Kolumna name z subs / z company z complaints | Nazwa firmy                                         |
| industry                   | numeric, not null              | sic z tabeli sub                             | Kod odpowiadający gałęzi przemysłu, do której należy firma |
| complaintcentdateid        | bigint, not null               | Na podstawie kolumny Date sent to company z complaints | Klucz obcy do tabeli dat                    |
| complaintreciveddateid     | bigint, not null               | Na podstawie kolumny Date received z complaints | Klucz obcy do tabeli dat                    |


## complaints_dim

| **Nazwa kolumny**         | **Typ**                         | **Skąd dane**                                | **Cel**                                             |
|---------------------------|---------------------------------|----------------------------------------------|-----------------------------------------------------|
| complaintid                | numeric, not null              | Complaint ID z complaints                    | Klucz główny tej tabeli                             |
| product                    | character varying(512), not null| Kolumna Product z complaints                 | Typ produktu finansowego, którego skarga dotyczy    |
| subproduct                 | character varying(512), not null| Kolumna SubProduct z complaints, braki danych zapełnić jako ‘not provided’ | Pod-typ produktu finansowego, którego skarga dotyczy |
| issue                      | character varying(512), not null| Kolumna Issue z complaints, braki danych zapełnić jako ‘not provided’ | Problem, którego dotyczy skarga                     |
| subissue                   | character varying(512), not null| Kolumna SubIssue z complaints, braki danych zapełnić jako ‘not provided’ | Pod-typ problemu, którego dotyczy skarga            |
| customertag                | character varying(512), not null| Kolumna Tag z complaints, braki danych zapełnić jako ‘standard’ | Dodatkowy tag definiujący klienta                   |
| customerconsent            | character varying(512), not null| Kolumna Consumer consent provided z complaints, braki danych zapełnić jako “NA” | Informacja o zgodzie konsumenta |
| submissionmethod           | character varying(512), not null| Kolumna Submitted via z complaints           | Sposób przesłania skargi                            |
| didcompanyrespondpublicly  | boolean, not null              | Kolumna Did company respond publicly z complaints | Czy firma odpowiedziała publicznie              |
| timelyresponse             | boolean, not null              | Kolumna Timely response? z complaints        | Czy odpowiedź była terminowa                        |
| consumerdisputed           | character varying(512), not null| Kolumna Consumer disputed? z complaints, Yes, No, NA | Czy konsument zakwestionował                       |
| consumerstate              | character varying(512), not null| Kolumna state z complaints                   | Lokalizacja zgłaszającego                           |
| consumerzipcode            | character varying(512), not null| Kolumna zip code z complaints                | Lokalizacja zgłaszającego                           |


## company_dim (SCD 2)

| **Nazwa kolumny**         | **Typ**                         | **Skąd dane**                                | **Cel**                                             |
|---------------------------|---------------------------------|----------------------------------------------|-----------------------------------------------------|
| companyname               | character varying(255), not null| Kolumna name z subs / z company z complaints | Nazwa firmy                                         |
| industry                  | integer, not null              | sic z tabeli sub                             | Kod odpowiadający gałęzi przemysłu, do której należy firma |
| countryname               | character varying(255), not null| Na podstawie kolumny countryba z sub         | Adres biznesowy firmy                               |
| statename                 | character varying(255), not null| Na podstawie kolumny stprba z sub            | Adres biznesowy firmy                               |
| cityname                  | character varying(255), not null| Na podstawie kolumny cityba z sub            | Adres biznesowy firmy                               |
| zipcode                   | character varying(255), not null| Na podstawie kolumny zipba z sub             | Adres biznesowy firmy                               |
| street                    | character varying(255), not null| Na podstawie kolumny bas1 z sub              | Adres biznesowy firmy                               |
| street2                   | character varying(255), not null| Na podstawie kolumny bas2 z sub              | Adres biznesowy firmy                               |
| countryregistered         | character varying(255), not null| Na podstawie kolumny countryinc z sub, braki danych zapełnić jako “NA” | Kraj zarejestrowania podmiotu         |
| stateregistered           | character varying(255), not null| Na podstawie kolumny stprinc z sub, braki danych zapełnić jako “NA” | Stan zarejestrowania podmiotu           |
| companyphonenumber        | character varying(255), not null| Na podstawie kolumny baph z sub, braki danych zapełnić jako “NA”, ujednolicić sposób zapisu | Nr. tel. biznesowy |
| cik                       | numeric, not null              | cik z sub                                    | Numer Centralnego Indeksu (CIK). To dziesięciocyfrowy numer nadawany przez SEC każdemu zarejestrowanemu podmiotowi składającemu dokumenty |
| startdate                 | bigint, not null               | Data wprowadzenia zmian w danych firmy, klucz obcy do tabeli dat | |
| enddate                   | bigint, not null               | Data wprowadzenia nowych zmian do danych, koniec świata jeśli aktywne, klucz obcy do tabeli dat | |
| companyid                 | numeric, not null              | Własna, shashowane połączenie nazwy i startdate | Klucz główny tej tabeli (zastępczy)               |
| isactive                  | boolean, not null              | Własna                                       | Czy dane firmy są aktualne                          |
| formername                | character varying(255), not null| Kolumna former z tabeli subs                 | Poprzednia nazwa firmy                              |

# ETL

## Dane finansowe

![image](https://github.com/user-attachments/assets/cb2cf4f3-988c-4250-a5e1-140cccabbd46)

*Rysunek 1: Schemat dagu, który insertuje dane finansowe firm*

Na początku wykonuje się DAG (`fin_statements_dag_id`), który przetwarza dane i tworzy cztery czyste zbiory danych:
- `tags.csv`
- `fact_item.csv`
- `company_dim.csv`
- `statement_dim.csv`

DAG jest ustawiony, aby uruchamiał się o północy co kwartał. Następnie DAG `check_last_dag_run_status` o godzinie drugiej sprawdza, czy DAG `fin_statements_dag_id` zakończył się sukcesem. Jeśli nie, to przerywa działanie (skip_tasks), a jeśli zakończył się sukcesem, to zaczyna insertować dane. Na początku wstawia dane do tabel wymiarów, a na końcu, po pomyślnym zakończeniu, do tabel faktów.

Poniżej przedstawiamy schematy wstawiania danych. Pominięto te prostsze, w których nie stosowano żadnych lookupów i filtrowania.

Możemy również wywołać DAG `fin_statements_dag_id` dla wcześniejszej daty. Automatycznie obliczamy kwartał i rok, a także otwieramy folder z kwartałem, do którego należy dana data.
```bash
airflow dags trigger -c '{"execution_date": "2023-10-20"}' fin_statements_dag_id
```
### insert_company_dim_data_task

![image](https://github.com/user-attachments/assets/659bc1b0-02e4-4337-b960-4ad693f3cc48)

**Proces ETL z SCD2 dla tabeli company_dim**

Podział na przypadki (1) i (2) pokrywa przedział dat w całości, ponieważ startdate i enddate przyjmują wartości domyślne, tzn. startdate wynosi 19000101 jeśli dane firmy nie zostały nigdy zmienione, a enddate wynosi 21001231 jeśli dane są aktualne. Na przykład, możemy mieć przedział [20001212, 21001231] i filled date 20201212 -> rozbijamy przedziały dat na [20001212, 20201212] oraz [20201212, 21001231].

### insert_fact_item_data_task

![image](https://github.com/user-attachments/assets/b388b379-c4e6-487c-ad10-92b5741b236f)

Wykonujemy insert takich danych, tak aby w hurtowni znalazły się tylko wiersze, których klucze obce odnoszą się do pewnych wartości w tabelach wymiarów. Połaczenie do tabeli 'tags' jest proste, ponieważ skorzystałyśmy z już istniejącego klucza z danych źródłowych. W procesie ETL filtrujemy tylko te tagi, dla których nie istnieją żadne informacje w danych źródłowych. 

Z kolei połączenie z tabelą wymiarów firmy obejmuje znalezienie nazwy firmy, oraz dopasowanie dat tak, aby tabela faktów odwoływała się do danych, które były aktualne dla wartości liczbowych. Dane liczbowe niekoniecznie muszą się odwoływać do punktu w czasie obejmującego ostatnie 3 miesiące, ponieważ zeznanie może być poprawką poprzedniego zeznania finansowego. Dodatkowo, dane firm mogą się zmieniać bardzo dynamicznie. Dlatego wybieramy taką firmę w której okres, dla którego dane były aktualne zawiera datę zakończenia ważności danej wartości liczbowej. 

### Skargi
![image](https://github.com/user-attachments/assets/86f41144-c3c6-4c9c-bb6f-db0adafd10d4)

Na początku o północy codziennie wykonuje się DAG `complaints_dag_id`. Tworzy on zbiory danych:

- `complaints_dim.csv`
- `complaints_fact.csv`

DAG `check_last_dag_run_status` sprawdza jego status oraz status ostatniego wykonania DAG `check_fin_statements_dag` o godzinie drugiej w nocy. Sprawdzenie statusu wykonania zadań insertowania danych finansowych jest niezbędne, aby odpowiednio połączyć dane faktowe z danymi firm.

Jeśli oba wskazane DAGi zakończyły się pomyślnie, `check_last_dag_run_status` przechodzi do wstawienia danych do tabeli.

![image](https://github.com/user-attachments/assets/183b79ef-7a00-4d2e-b038-edcc66bd2012)

### Przykładowa wizualizacja

![Messenger_creation_221c53a6-8227-4810-a659-be8a72c12061](https://github.com/user-attachments/assets/fce1804f-a438-4193-80e4-3475e0f20125)


