# Cloud Computing Project of group CloudiPi: Motore di Ricerca con Inverted Index

## Obiettivo

Il progetto prevede la realizzazione di un **inverted index** a partire da una collezione di file. L'obiettivo è realizzare un sistema scalabile ed estensibile, in grado di indicizzare testi e rispondere a query testuali.

## Esempio di Inverted Index

```
cloud      doc1.txt:1  doc2.txt:1
computing  doc1.txt:1  doc2.txt:1
is         doc1.txt:1
important  doc2.txt:1
```

## Requisiti principali

- Installare **Hadoop in modalità completamente distribuita** (fully distributed mode).
- Implementare due versioni dell’inverted index:
  - Una utilizzando **Hadoop (Java)**
  - Una utilizzando **Spark (Python)**
- Per ogni parola rilevata, l’output dell’inverted index deve includere:
  - I nomi dei file in cui compare la parola
  - Il numero di occorrenze della parola in ciascun file

- Implementare la logica di **combiner**
- Utilizzare i metodi `setup()` e `cleanup()`, se appropriato
- Realizzare un semplice **motore di ricerca in Python (non parallelo)** che:
  - Riceve una query testuale
  - Restituisce **solo i nomi dei file** che contengono **tutti** i termini della query (non deve indicare il numero di occorrenze)

- Usare una collezione di file a scelta (es. articoli, libri, pagine web), con dimensioni variabili da **pochi KB a diversi GB**
- Eseguire una **valutazione comparativa delle performance**, raccogliendo:
  - Tempo di esecuzione
  - Uso di memoria
  - Altri indicatori rilevanti
- Redigere una **relazione tecnica** (massimo 4–5 pagine), contenente:
  - Pseudocodice della soluzione MapReduce in Hadoop
  - Descrizione dei dataset
  - Analisi sperimentale

## Requisiti aggiuntivi

- Implementare la logica di **in-mapper combining**
- Sviluppare una **versione parallela in Python** per la costruzione dell’inverted index (usando multiprocessing o librerie parallele)
- Aumentare il **numero di reducer** e raccogliere statistiche relative all'esecuzione
