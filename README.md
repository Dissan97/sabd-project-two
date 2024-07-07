SABD Project 2
## Autore
Dissan Uddin Ahmed 
## Anno Accademico
2023/2024

# Requisiti del progetto
Lo scopo del progetto e rispondere ad alcune query su dati di telemetria di circa 200k hard disk nei data center gestiti da Backblaze [1], utilizzando l’approccio di processamento a stream con Apache Flink. Per gli scopi di questo progetto, viene fornita una versione ridotta del dataset indicato nel Grand Challenge della conferenza ACM DEBS 2024. Il dataset riporta i dati di monitoraggio S.M.A.R.T.2, esteso con alcuni attributi catturati da Backblaze. Il dataset contiene eventi riguardanti circa 200k hard disk, dove ogni evento riporta lo stato S.M.A.R.T. di un particolare hard disk in uno specifico giorno. Il dataset ridotto contiene circa 3 milioni di eventi (a fronte dei 5 milioni del dataset originario). Le query a cui rispondere in modalita` streaming sono:

# Query 1
Per i vault (campo vault id) con identificativo compreso tra 1000 e 1020, calcolare il numero di eventi, il valor medio e la deviazione standard della temperatura misurata sui suoi hard disk (campo s194 temperature celsius). Si faccia attenzione alla possibile presenza di eventi che non hanno assegnato un valore per il campo relativo alla temperatura. Per il calcolo della deviazione standard, si utilizzi un algoritmo online, come ad esempio l’algoritmo di Welford3 . Calcolare la query sulle finestre temporali: • 1 giorno (event time) • 3 giorni (event time); • dall’inizio del dataset. L’output della query ha il seguente schema: ts, vault id, count, mean s194, stddev s194 dove: • ts: timestamp relativo all’inizio della finestra su cui e stata calcolata la statistica; • vault id: identificativo del vault; • count: numero di misurazioni; • mean s194: valor medio della temperatura nella finestra; • stddev s194: (stimatore della) deviazione standard della temperatura nella finestra

# Query 2
Calcolare la classifica aggiornata in tempo reale dei 10 vault che registrano il piu alto numero di falli menti nella stessa giornata. Per ogni vault, riportare il numero di fallimenti ed il modello e numero seriale degli hard disk guasti. Calcolare la query sulle finestre temporali: • 1 giorno (event time) • 3 giorni (event time); • dall’inizio del dataset. L’output della query ha il seguente schema: ts, vault id1, failures1 ([modelA, serialA, ...]), ..., vault id10, failures10 ([modelZ, serialZ, ...]) dove: • ts: timestamp relativo all’inizio della finestra su cui e stata calcolata la classifica; • vault id[1-10]: identificativo del vault in posizione [1-10] nella classifica top-10; • failures[1-10]: numero di fallimenti registrati per il vault con vault id[1-10] nella finestra considerata; • [modelA, serialA, ...]: lista di modelli e numeri seriali degli hard disk guasti per il vault di riferimento

# Script Utili per lanciare correttamente il progetto: