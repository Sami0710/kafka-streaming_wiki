# kafka-streaming

Pipeline temps réel sur les modifications de Wikipédia : le flux public de Wikimedia passe par Kafka, est stocké dans PostgreSQL et affiché dans un dashboard Streamlit.

```mermaid
flowchart LR
    W[Wikimedia EventStreams] --> P[producer]
    P --> K[(Kafka)]
    K --> C[consumer]
    C --> DB[(PostgreSQL)]
    DB --> D[dashboard]
```

## Lancer le projet

Il faut Docker.

```bash
docker compose up -d --build
```

- Dashboard : http://localhost:8501
- Kafka UI : http://localhost:8080

Pour arrêter : `docker compose down` (ajouter `-v` pour supprimer aussi la base).

## Services

| Service | Rôle |
|---|---|
| producer | lit le flux SSE de Wikimedia et publie dans le topic `wiki.changes` |
| kafka | broker Kafka 3.8 en mode KRaft, 3 partitions |
| consumer | lit le topic par lots et insère dans PostgreSQL |
| postgres | stockage des modifications (table `wiki_changes`) |
| dashboard | Streamlit, rafraîchi toutes les 5 s |
| kafka-ui | inspection des topics et des messages |

## Quelques choix

- La clé des messages est le nom du wiki (`frwiki`, `enwiki`...), ce qui garde l'ordre des modifications pour un même wiki.
- Le consumer commit ses offsets après l'insertion en base. Si il crash entre les deux, les messages sont relus, et `ON CONFLICT (id) DO NOTHING` évite les doublons.
- Le producer renvoie `Last-Event-ID` quand il se reconnecte, pour reprendre le flux là où il s'était arrêté.
- Le parsing des événements est dans `src/wiki.py`, séparé du réseau, pour pouvoir le tester.

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```

Les tests tournent aussi dans GitHub Actions à chaque push.

## À faire

- purger ou agréger les vieilles données (environ 2 millions de lignes par jour)
- détecter les pages modifiées beaucoup de fois en peu de temps
- schéma Avro + Schema Registry
