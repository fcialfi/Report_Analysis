# Report_Analysis

> **⚠️ Progetto spostato.** Questo codice ora fa parte di
> [MEOS-Extract](https://github.com/fcialfi/MEOS-Extract) come `report_analysis.py`
> (con tutta la cronologia), utilizzabile da riga di comando e dalla GUI
> (opzione *Analisi multi-orbita* e pulsante *Analizza Excel già estratti…*).
> Questo repository è archiviato e non riceve più aggiornamenti.

Analisi multi-orbita degli Excel prodotti da [MEOS-Extract](https://github.com/fcialfi/MEOS-Extract):
confronta il rapporto segnale/rumore (SNR) di passaggi diversi in funzione della posizione dell'antenna.

## Cosa fa

`process_meos_extract.py` legge tutti gli Excel `*_orbit_<numero>.xlsx` di una cartella e per ogni orbita:

1. prende le serie **SNR**, **azimuth** ed **elevation** (sia formato vecchio con fogli `6_1_azimuth`/`6_2_elevation`,
   sia formato nuovo con un unico foglio antenna `<sezione>_azimuth`/`<sezione>_elevation`);
2. abbina ogni campione SNR all'azimuth/elevation più vicino nel tempo (entro 2 s);
3. taglia la coda dopo la fine del passaggio (antenna che torna in parcheggio: tutto ciò che segue l'ultimo campione ≤ 5°);
4. divide il passaggio in **salita** (prima del picco di elevazione) e **discesa** (dopo).

Poi raggruppa le orbite che ripercorrono la stessa traccia a terra: l'orbita si ripete ogni **9 giorni**, quindi
passaggi il cui orario di inizio cade nella stessa posizione del ciclo (entro ±30 min) finiscono nello stesso gruppo.

L'output (`meos_processed.xlsx`) contiene per ogni gruppo:

- foglio **Group N**: per ogni orbita tempo, azimuth, elevation, SNR ed elevazione con segno (negativa in salita);
  sotto, la tabella *SNR Correlation* con l'SNR delle orbite nei punti azimuth/elevation comuni (con relativo grafico);
- foglio **Group N Chart**: due grafici SNR vs elevazione, uno per la salita e uno per la discesa, una serie per orbita.

Se il file di output esiste già, le orbite che contiene vengono mantenute e unite a quelle nuove
(un'orbita presente in entrambi viene aggiornata con i dati nuovi): si possono quindi aggiungere orbite nel tempo.
Gli Excel della cartella che non sono export MEOS (es. `lock_state_stats.xlsx`) vengono saltati con un avviso.

## Uso

```bash
pip install pandas openpyxl
python process_meos_extract.py <cartella_excel> -o meos_processed.xlsx
```

Opzioni:

| Opzione | Default | Significato |
|---|---|---|
| `-o`, `--output` | `meos_processed.xlsx` | file Excel di output |
| `--repeat-days` | `9` | ciclo di ripetizione della traccia a terra (giorni) |
| `--group-tolerance-min` | `30` | scarto massimo (minuti) nel ciclo per stare nello stesso gruppo |
| `--azimuth-tolerance` | `0.1` | passo (gradi) dei bin di azimuth della tabella di correlazione |
| `--elevation-tolerance` | `0.1` | passo (gradi) dei bin di elevation della tabella di correlazione |
| `--replace` | – | ignora l'output esistente e lo rigenera solo dagli Excel della cartella |
