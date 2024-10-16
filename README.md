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

| **Nazwa kolumny**         | **Typ**                         | **Skąd dane**                                | **Cel**                                             |
|---------------------------|---------------------------------|----------------------------------------------|-----------------------------------------------------|
| factitemid                | SERIAL, NOT NULL                | własna                                       | Klucz główny tej tabeli                             |
| statementid               | NUMERIC, NOT NULL               | Kolumna “adsh” z tabeli sub                  | Klucz obcy do tabeli                                |
| companyid                 | NUMERIC, NOT NULL               | własna, shashowane połączenie nazwy i startdate | Klucz obcy do tabeli CompanyDim                  |
| tagid                     | NUMERIC, NOT NULL               | własna, shashowane połączenie tagu i wersji   | Klucz obcy do tabeli TagDim                         |
| companyname               | VARCHAR(512), NOT NULL          | Kolumna name z subs                          | Nazwa firmy                                         |
| coregistrantname           | VARCHAR(512), NOT NULL          | Na podstawie coreg z num                     | Coregistrant, dodana tutaj, ponieważ identyfikuje wartość |
| value                     | DOUBLE PRECISION, NOT NULL      | Kolumna value z tabeli nums                  | Wartość pieniężna                                   |
| unitofmeasure             | VARCHAR(255), NOT NULL          | Kolumna uom z tabeli nums                    | Rodzaj wartości np. waluta                         |
| enddateid                 | BIGINT, NOT NULL                | Kolumna ddate z nums                         | Koniec okresu do którego ta wartość obowiązuje      |
| startdateid               | BIGINT, NOT NULL                | Obliczone z ddate i qtrs z tabeli nums       | Początek okresu ważności tej wartości               |

## statement_dim

| **Nazwa kolumny**         | **Typ**                         | **Skąd dane**                                | **Cel**                                             |
|---------------------------|---------------------------------|----------------------------------------------|-----------------------------------------------------|
| statementid               | NUMERIC, NOT NULL               | Kolumna “adsh” z tabeli sub                  | Klucz główny tej tabeli                             |
| fiscalyear                | INTEGER, NULL                   | Kolumna fy z tabeli subs                     | Rok fiskalny                                        |
| fiscalperiod              | VARCHAR(255), NULL              | Kolumna fp z tabeli subs                     | Okres fiskalny                                      |
| formtype                  | VARCHAR(255), NOT NULL          | Kolumna form z tabeli subs                   | Rodzaj złożonego zeznania                           |
| balancesheetdateid        | BIGINT, NOT NULL                | Kolumna period z tabeli subs                 | Zwykle koniec okresu obejmującego zgłoszenie        |
| filleddateid              | BIGINT, NOT NULL                | Kolumna filed z tabeli subs                  | Kiedy zeznanie zostało złożone                      |
| accepteddateid            | BIGINT, NOT NULL                | Kolumna accepted z tabeli subs               | Kiedy zeznanie zostało zaakceptowane                |
| previousreport            | BOOLEAN, NOT NULL               | Kolumna prevrpt z tabeli subs                | Czy to zeznanie zostało poprawione                  |
| numbercik                 | NUMERIC, NOT NULL               | Kolumna nicks z tabeli subs                  | Ilość firm, które obejmuje zeznanie                 |

## tags

| **Nazwa kolumny**         | **Typ**                         | **Skąd dane**                                | **Cel**                                             |
|---------------------------|---------------------------------|----------------------------------------------|-----------------------------------------------------|
| tagid                     | NUMERIC, NOT NULL               | Własna, shashowane połączenie tagu i wersji  | Klucz główny                                        |
| tag                       | VARCHAR(512), NOT NULL          | Kolumna tag z tabeli tag                     | Nazwa tagu                                          |
| version                   | VARCHAR(512), NOT NULL          | Kolumna version z tabeli tag                 | Wersja tagu                                         |
| tagdescription            | TEXT, NOT NULL                  | Kolumna tlabel z tabeli tag                  | Ładna nazwa tagu                                    |
| type                      | VARCHAR(512), NOT NULL          | Stworzone na podstawie kolumn datatype, crdr, abstract z tabeli tag | Określa, czy credit, debit, czy to nie jest wartość monetarna |

## company_complaints_fact

| **Nazwa kolumny**         | **Typ**                         | **Skąd dane**                                | **Cel**                                             |
|---------------------------|---------------------------------|----------------------------------------------|-----------------------------------------------------|
| factid                    | SERIAL, NOT NULL                | Własna                                       | Klucz główny tej tabeli                             |
| complaintid               | NUMERIC, NOT NULL               | Complaint ID z complaints                    | Klucz obcy do tabeli complaints_dim                 |
| companyid                 | NUMERIC, NOT NULL               | Własna                                       | Klucz obcy do tabeli company_dim                    |
| companyname               | VARCHAR(512), NOT NULL          | Kolumna name z subs / z company z complaints | Nazwa firmy                                         |
| industry                  | NUMERIC, NOT NULL               | sic z tabeli sub                             | Kod odpowiadający gałęzi przemysłu, do której należy firma |
| complaintcentdateid        | BIGINT, NOT NULL               | Na podstawie kolumny Date sent to company z complaints | Klucz obcy do tabeli dat                    |
| complaintreciveddateid     | BIGINT, NOT NULL               | Na podstawie kolumny Date received z complaints | Klucz obcy do tabeli dat                    |

## complaints_dim

| **Nazwa kolumny**         | **Typ**                         | **Skąd dane**                                | **Cel**                                             |
|---------------------------|---------------------------------|----------------------------------------------|-----------------------------------------------------|
| complaintid                | NUMERIC, NOT NULL               | Complaint ID z complaints                    | Klucz główny tej tabeli                             |
| product                    | VARCHAR(512), NOT NULL          | Kolumna Product z complaints                 | Typ produktu finansowego, którego skarga dotyczy    |
| subproduct                 | VARCHAR(512), NOT NULL          | Kolumna SubProduct z complaints, braki danych zapełnić jako ‘not provided’ | Pod-typ produktu finansowego, którego skarga dotyczy |
| issue                      | VARCHAR(512), NOT NULL          | Kolumna Issue z complaints, braki danych zapełnić jako ‘not provided’ | Problem, którego dotyczy skarga                     |
| subissue                   | VARCHAR(512), NOT NULL          | Kolumna SubIssue z complaints, braki danych zapełnić jako ‘not provided’ | Pod-typ problemu, którego dotyczy skarga            |
| customertag                | VARCHAR(512), NOT NULL          | Kolumna Tag z complaints, braki danych zapełnić jako ‘standard’ | Dodatkowy tag definiujący klienta                   |
| customerconsent            | VARCHAR(512), NOT NULL          | Kolumna Consumer consent provided z complaints, braki danych zapełnić jako “NA” | Informacja o zgodzie konsumenta |
| submissionmethod           | VARCHAR(512), NOT NULL          | Kolumna Submitted via z complaints           | Sposób przesłania skargi                            |
| didcompanyrespondpublicly  | BOOLEAN, NOT NULL               | Kolumna Did company respond publicly z complaints | Czy firma odpowiedziała publicznie              |
| timelyresponse             | BOOLEAN, NOT NULL               | Kolumna Timely response? z complaints        | Czy odpowiedź była terminowa                        |
| consumerdisputed           | VARCHAR(512), NOT NULL          | Kolumna Consumer disputed? z complaints, Yes, No, NA | Czy konsument zakwestionował                       |
| consumerstate              | VARCHAR(512), NOT NULL          | Kolumna state z complaints                   | Lokalizacja zgłaszającego                           |
| consumerzipcode            | VARCHAR(512), NOT NULL          | Kolumna zip code z complaints                | Lokalizacja zgłaszającego                           |

## company_dim (SCD 2)

| **Nazwa kolumny**         | **Typ**                         | **Skąd dane**                                | **Cel**                                             |
|---------------------------|---------------------------------|----------------------------------------------|-----------------------------------------------------|
| companyname               | VARCHAR(255), NOT NULL          | Kolumna name z subs / z company z complaints | Nazwa firmy                                         |
| industry                  | INTEGER, NOT NULL               | sic z tabeli sub                             | Kod odpowiadający gałęzi przemysłu, do której należy firma |
| countryname               | VARCHAR(255), NOT NULL          | Na podstawie kolumny countryba z sub         | Adres biznesowy firmy                               |
| statename                 | VARCHAR(255), NOT NULL          | Na podstawie kolumny stprba z sub            | Adres biznesowy firmy                               |
| cityname                  | VARCHAR(255), NOT NULL          | Na podstawie kolumny cityba z sub            | Adres biznesowy firmy                               |
| zipcode                   | VARCHAR(255), NOT NULL          | Na podstawie kolumny zipba z sub             | Adres biznesowy firmy                               |
| street                    | VARCHAR(255), NOT NULL          | Na podstawie kolumny bas1 z sub              | Adres biznesowy firmy                               |
| street2                   | VARCHAR(255), NOT NULL          | Na podstawie kolumny bas2 z sub              | Adres biznesowy firmy                               |
| countryregistered         | VARCHAR(255), NOT NULL          | Na podstawie kolumny countryinc z sub, braki danych zapełnić jako “NA” | Kraj zarejestrowania podmiotu         |
| stateregistered           | VARCHAR(255), NOT NULL          | Na podstawie kolumny stprinc z sub, braki danych zapełnić jako “NA” | Stan zarejestrowania podmiotu           |
| companyphonenumber        | VARCHAR(255), NOT NULL          | Na podstawie kolumny baph z sub, braki danych zapełnić jako “NA”, ujednolicić sposób zapisu | Nr. tel. biznesowy |
| cik                       | NUMERIC, NOT NULL               | cik z sub                                    | Numer Centralnego Indeksu (CIK). To dziesięciocyfrowy numer nadawany przez SEC każdemu zarejestrowanemu podmiotowi składającemu dokumenty |
| startdate                 | BIGINT, NOT NULL                | Data wprowadzenia zmian w danych firmy, klucz obcy do tabeli dat | |
| enddate                   | BIGINT, NOT NULL                | Data wprowadzenia nowych zmian do danych, koniec świata jeśli aktywne, klucz obcy do tabeli dat | |
| companyid                 | NUMERIC, NOT NULL               | Własna, shashowane połączenie nazwy i startdate | Klucz główny tej tabeli (zastępczy)               |
| isactive                  | BOOLEAN, NOT NULL               | Własna                                       | Czy dane firmy są aktualne                          |
| formername                | VARCHAR(255), NOT NULL          | Kolumna former z tabeli subs                 | Poprzednia nazwa firmy                              |

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

### Przykładowe wizualizacje

![image](https://github.com/user-attachments/assets/9214b7f9-cdd9-4db4-be32-a9af5520c509)

![image](https://github.com/user-attachments/assets/c17bc85f-82be-447e-9e89-701d7f9c7e66)

![image](https://github.com/user-attachments/assets/ebdf2a64-d6a4-4f1f-9d1a-fc57cb6a7658)


