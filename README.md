# Analiza kvaliteta vazduha u Beogradu — Spark Air Quality Analysis

Ovaj projekat koristi Apache Spark za naprednu obradu i analizu podataka o kvalitetu vazduha u Beogradu, sa posebnim fokusom na koncentracije PM2.5 čestica, temperaturu i relativnu vlažnost vazduha. Podaci su preuzeti sa globalne platforme OpenAQ, sa merenja senzora na lokaciji Belgrade – BIG Fashion Park, tokom jednogodišnjeg perioda sa satnim rezolucijama. Projekat prikazuje primenu Spark-a za integraciju, agregaciju i kategorizaciju podataka, uz identifikaciju vremenskih i meteoroloških obrazaca zagađenja.

---

## Funkcionalnosti

1) **Učitavanje i inicijalna obrada podataka**
   - Učitavanje CSV fajla (`air_quality_belgrade.csv`) sa OpenAQ platforme.
   - Konverzija tipova podataka i standardizacija vremenskih oznaka.

2) **Filtriranje i integracija parametara**
   - Izdvajanje relevantnih pokazatelja: PM2.5 (`pm25`), temperatura (`temperature`) i relativna vlažnost (`relativehumidity`).
   - Spajanje podataka u jedinstvenu tabelu po vremenskoj oznaci (`datetime`).

3) **Izvlačenje vremenskih karakteristika**
   - Dodavanje kolona: sat (`hour`), datum (`date`), mesec (`month`), godišnje doba (`season`).

4) **Analiza PM2.5 po temperaturnim zonama**
   - Kategorizacija temperature u zone: "do 15°C", "15–30°C" i "preko 30°C".
   - Izračunavanje prosečne koncentracije PM2.5 po temperaturnim zonama.

5) **Prosečna koncentracija PM2.5 po mesecima**
   - Statistička analiza prosečnih vrednosti PM2.5 za svaki mesec u godini.

6) **Prosečna koncentracija PM2.5 po godišnjim dobima**
   - Grupisanje i agregacija PM2.5 po sezonama: Proleće, Leto, Jesen, Zima.

7) **Prosečna koncentracija PM2.5 po satima u danu**
   - Analiza dnevnog obrasca zagađenja PM2.5 kroz prosečne vrednosti po satima.

8) **Analiza PM2.5 po opsezima relativne vlažnosti**
   - Grupisanje PM2.5 po opsezima vlažnosti u intervalima od 10%.

9) **Identifikacija najzagađenijeg dana**
   - Pronalaženje dana sa najvišom prosečnom koncentracijom PM2.5, uz analizu prosečne temperature i vlažnosti za taj dan.

10) **Kategorizacija zimskih dana po indeksu kvaliteta vazduha**
    - Klasifikacija zimskih dana po kategorijama kvaliteta vazduha na osnovu vrednosti PM2.5, korišćenjem AQI granica ("dobar", "umeren", "nezdrav za osetljive", "nezdrav", "vrlo nezdrav", "opasan").

---

## Skup podataka

- Izvor: [OpenAQ](https://openaq.org/)
- Korišćeni fajl: `air_quality_belgrade.csv`
  - Parametri: datum i vreme merenja (`datetimeUtc`), tip parametra (`parameter`: pm25, temperature, relativehumidity), izmerena vrednost (`value`).
  - Lokacija merenja: Belgrade – BIG Fashion Park (urbana zona, blizina saobraćaja i komercijalnih centara).
- Vremenski okvir: satna merenja tokom jedne godine.

---

## Prethodni uslovi

- **Java JDK 17** (preporučeno) ili JDK 11/17 kompatibilan sa Spark 3.x
- **Apache Spark 3.x** (lokalni režim je dovoljan)
- **Scala** (kompatibilna sa Spark-om, tipično 2.12.x)
- **Eclipse** sa Scala plugin-om (preporučeno za razvoj)
- **Dodatne JVM dozvole** (zbog Spark-a i JDK modula):
   ```
   --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
   --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
   ```
- Preuzeti `air_quality_belgrade.csv` i smestiti ga u lokalni folder (npr. `dataset/` u korenu projekta).

---

## Pokretanje u Eclipse-u (preporučeno)

1) **Preuzimanje i raspakivanje**
   - Na GitHub-u klikni na “Code” → “Download ZIP”.
   - Raspakuj ZIP na lokalni disk (npr. `C:\projekti\spark-air-quality-bg`).

2) **Import projekta**
   - Pokreni Eclipse → File → Open Projects from File System... → Directory… → izaberi folder projekta → Eclipse prepoznaje projekat → Finish

3) **Podesite JDK**
   - Project → Properties → Java Build Path → koristite JDK 17 (ili kompatibilan sa Spark 3.x).

4) **Dodajte VM argumente (OBAVEZNO)**
   - Run → Run Configurations… → (izaberite main klasu `air_quality_bg`) → tab: Arguments → VM arguments:
     ```
     --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
     ```
   - Dodajte isto za svaku main klasu koju pokrećete.

5) **Pokretanje**
   - U Project Explorer-u klikni na `air_quality_bg` → desni klik → Run As → Java Application
   - Izlaz rezultata će se pojaviti u Console prozoru.

---

## Detalji implementacije

- **Glavna klasa:**  
  - `package rs.raf.pds.spark;`
  - `public class air_quality_bg` — sadrži kompletnu implementaciju analitičkih zadataka.
  - Korišćenje Spark SQL API-ja za učitavanje, filtriranje, agregaciju i kategorizaciju podataka.
  
- **Obrada podataka:**
  - Učitavanje CSV fajla sa header-ima i automatskom inferencijom tipova.
  - Filtriranje po `parameter` vrednostima (pm25, temperature, relativehumidity).
  - Spajanje podataka po vremenskom atributu.
  - Dodavanje vremenskih kolona (`hour`, `date`, `month`, `season`) radi vremenske analize.
  - Grupisanje i agregacija po različitim vremenskim i meteorološkim kriterijumima.
  - Kategorizacija vrednosti prema međunarodnim standardima kvaliteta vazduha (AQI).

- **Primeri rezultata:**
  - Prosečne koncentracije PM2.5 po temperaturnim zonama, mesecima, sezonama, satima, vlažnosti.
  - Identifikacija najzagađenijeg dana sa pratećim temperaturama i vlažnostima.
  - Broj zimskih dana po kategorijama kvaliteta vazduha.
