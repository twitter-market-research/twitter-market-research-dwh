# Documentation API X — Twitter Market Research Ligue 1

**Date** : 26 mars 2026
**Auteur** : Djaba Semakia — Data Engineer
**Projet** : Twitter Market Research — Ligue 1
**Destinataires** : Direction, Finance, Produit

---

## 1. Resume executif

| Point | Message cle |
|---|---|
| **Pourquoi API X ?** | Seul acces fiable, en temps reel et legal aux donnees Twitter pour notre etude de marche Ligue 1 |
| **Cout mensuel estime** | ~200-250 USD (modele pay-per-use ou Basic tier) |
| **ROI attendu** | Analyse de 15 000 a 50 000 tweets/mois -> detection des themes prioritaires (VAR, analyse video, stats) avant lancement produit |
| **Alternatives au scraping** | Aucune viable legalement (risque de ban, donnees non structurees, pas de garantie d'echelle) |

**Recommandation** : Souscrire au **Basic tier (200 USD/mois)** ou au **pay-per-use** selon le volume reel.

---

## 2. Objectifs du projet

### 2.1 Contexte metier

Nous developpons une **application IA d'analyse video de matchs de Ligue 1**. Avant le lancement, nous devons valider :

1. **Le marche existe-t-il ?** — Y a-t-il une audience active sur X qui parle de VAR, d'analyse video, de stats de match ?
2. **Quels sont les themes dominants ?** — Quels sujets reviennent le plus (VAR, performance d'equipe, arbitrage, analyse video, stats tactiques) ?
3. **Quel est le sentiment ?** — Les fans sont-ils positifs, negatifs ou neutres sur ces themes ?
4. **Qui sont les influenceurs ?** — Quels comptes generent le plus d'engagement autour de ces sujets ?

### 2.2 Pourquoi X (Twitter) specifiquement

| Critere | X (Twitter) | Reddit | Forums | Google Trends |
|---|---|---|---|---|
| Temps reel | Oui (streaming) | Non | Non | Delai 1 jour |
| Volume Ligue 1 | Enorme (francophone) | Moyen | Faible | Non granulaire |
| Sentiment riche | Emojis, likes, replies | Mixte | Limite | Aucun |
| Hashtags structures | Oui (#Ligue1 #VAR #PSG) | Non | Non | Non |
| API officielle | Oui | Non | N/A | Oui (limite) |

**Conclusion** : X est la seule source qui combine **temps reel, volume, structuration et qualite** pour notre cas d'usage.

---

## 3. Architecture technique de notre pipeline

```
+---------------------------------------------------------------------+
|                         PROJET : TWEETS RAW                         |
+---------------------------------------------------------------------+
|                                                                     |
|  +-------------+    +-------------+    +---------------------+      |
|  | API X (v2)  |--->|  Producer   |--->|  Kafka (3 brokers)  |      |
|  | /tweets     |    |  Python     |    |  - tweets_raw       |      |
|  | /search     |    |             |    |  - tweets_enriched  |      |
|  +-------------+    +-------------+    |  - audit_logs       |      |
|                                         +----------+----------+      |
|                                                    |                 |
|                                                    v                 |
|  +--------------------------------------------------------------+   |
|  |                   Spark Structured Streaming                 |   |
|  |  - NLP (sentiment analysis)                                  |   |
|  |  - Extraction themes : VAR, performance, analyse video       |   |
|  |  - Agregation par club, par jour                             |   |
|  +--------------------------------------------------------------+   |
|                                                    |                 |
|                                                    v                 |
|  +--------------------------------------------------------------+   |
|  |                      MongoDB / Elasticsearch                 |   |
|  |  - tweets_enriched_db                                        |   |
|  |  - dashboard_metrics                                         |   |
|  +--------------------------------------------------------------+   |
|                                                    |                 |
|                                                    v                 |
|  +--------------------------------------------------------------+   |
|  |                    Grafana (Dashboard)                       |   |
|  |  - Volume tweets par jour/theme                              |   |
|  |  - Sentiment global                                          |   |
|  |  - Top clubs / Top influenceurs                              |   |
|  +--------------------------------------------------------------+   |
|                                                                     |
+---------------------------------------------------------------------+
```

### 3.1 Dependance cluster

| Composant | Role | Configuration |
|---|---|---|
| **Kafka** | Message broker temps reel | 3 brokers + Zookeeper (haute disponibilite) |
| **Prometheus** | Collecte des metriques | Scraping JMX Exporter sur chaque broker |
| **Grafana** | Visualisation + alerting | Dashboards volume, lag, sentiment |
| **Alertmanager** | Notification | Slack en cas de broker down ou consumer lag > 10 000 |

---

## 4. Acces a l'API X — Etats des lieux 2026

### 4.1 Evolution historique

| Date | Evenement | Impact |
|---|---|---|
| Fev 2023 | Fin du free tier (v1.1 et v2) | API devient 100% payante |
| Oct 2025 | Beta du pay-per-use | Nouvel acces au compte |
| Fev 2026 | Lancement officiel du pay-per-use | Abandon progressif des tarifs fixes |

### 4.2 Les 2 modeles disponibles aujourd'hui

#### Modele A — Pay-per-Use (nouveau, par defaut pour les nouveaux developpeurs)

| Operation | Cout unitaire |
|---|---|
| Lecture d'un tweet | 0.005 USD / tweet |
| Lecture d'un profil utilisateur | 0.01 USD / user |
| Lecture d'un evenement DM | 0.01 USD / DM |
| Publication d'un tweet | 0.01-0.015 USD / tweet |

**Bonus xAI** : pour chaque dollar depense sur l'API X, gagnez jusqu'a **20 % en credits xAI** (Grok).

#### Modele B — Tarifs fixes (encore disponibles pour les abonnes existants)

| Tier | Prix/mois | Tweets lecture | Tweets ecriture |
|---|---|---|---|
| Free | 0 USD | 0 | 1 500 |
| **Basic** | **200 USD** | **15 000** | **50 000** |
| Pro | 5 000 USD | 1 000 000 | 300 000 |
| Enterprise | 42 000 USD+ | Negociable | Negociable |

### 4.3 Notre recommandation de plan

**Scenario 1 — Demarrage (premier mois)**

Option retenue : **Pay-per-use** avec un credit initial de 500 USD (voucher beta).

| Usage estime | Cout estime |
|---|---|
| 10 000 tweets lus | 50 USD |
| 500 profils utilisateurs | 5 USD |
| Publication de rapports auto | 10 USD |
| **Total estime** | **~65 USD** (sous le seuil de 500 USD) |

**Scenario 2 — Croissance (mois 2+)**

Si le volume depasse 30 000 tweets/mois -> passer au **Basic (200 USD/mois)** pour un cout previsible.

**Scenario 3 — Scale (si l'etude de marche valide le projet)**

-> **Pro (5 000 USD/mois)** pour 1 M de tweets + acces au full-archive search + filtered stream en temps reel.

---

## 5. Analyse couts / benefices (ROI)

### 5.1 Couts sur 3 mois (Phase POC)

| Mois | Modele | Cout | Volume tweets |
|---|---|---|---|
| Mois 1 | Pay-per-use (voucher) | 0 USD (500 USD offert) | ~50 000 tweets |
| Mois 2 | Basic tier | 200 USD | 15 000 tweets |
| Mois 3 | Basic tier + top-up | 200 USD + 50 USD | 30 000 tweets |
| **Total** | | **450 USD** | **~100 000 tweets analyses** |

### 5.2 Benefices attendus

| Benefice | Impact |
|---|---|
| Validation du marche Ligue 1 | Evite un lancement produit sur un marche non valide (economie potentielle > 50K USD en dev) |
| Detection des themes prioritaires | Priorisation du roadmap produit (analyse video vs. stats vs. VAR) |
| Identification des influenceurs | Liste de comptes cibles pour la campagne marketing de lancement |
| Dataset historique enrichi | Actif reutilisable pour l'entrainement des modeles NLP internes |

### 5.3 Comparaison aux alternatives

| Solution | Cout mensuel | Qualite | Risque legal |
|---|---|---|---|
| **API X officielle** | 200-250 USD | Haute | Nul |
| Scraping (BeautifulSoup) | 0 USD | Moyenne (ban, captcha) | Eleve (TOS violation) |
| Twikit / Snscrape (open-source) | 0 USD | Moyenne | Eleve |
| API tierce (Social Data, Brand24) | 500-2 000 USD | Haute | Nul |

---

## 6. Endpoints API utilises dans notre pipeline

| Endpoint | Usage | Frequence estimee |
|---|---|---|
| GET /2/tweets/search/recent | Recherche hashtags #Ligue1, #VAR, clubs | 1 req/15min -> 96/jour |
| GET /2/tweets/search/stream | Stream temps reel des tweets | 1 connexion permanente |
| GET /2/tweets/:id | Recuperation details tweet | Par tweet collecte |
| GET /2/users/:id | Recuperation profil auteur | Par tweet collecte |
| GET /2/tweets/counts/recent | Volume de tweets par mot-cle | 1 req/15min |

### 6.1 Limites de taux (rate limits)

| Endpoint | Limite (15 min) | Max resultats |
|---|---|---|
| /2/tweets/search/recent | 450 | 100 par requete |
| /2/tweets/search/stream | 50 connexions | 1 connexion active |
| /2/tweets/counts/recent | 300 | - |

---

## 7. Procedure d'obtention des cles API

### Etape 1 — Creer un compte developpeur

1. Aller sur https://developer.x.com
2. S'authentifier avec son compte X
3. Accepter le Developer Agreement

### Etape 2 — Creer une application

1. Dans le Developer Portal -> **Create Project**
2. Nom du projet : `twitter-market-research-ligue1`
3. Type d'usage : **App** (pour consommation de donnees)
4. Souscrire au **pay-per-use** (defaut) ou au **Basic (200 USD/mois)**

### Etape 3 — Generer les cles

Dans le Developer Portal -> ton projet -> **Keys and tokens** :

| Cle | Role | Securite |
|---|---|---|
| `BEARER_TOKEN` | Acces OAuth 2.0 (lecture) | Variable d'environnement `X_BEARER_TOKEN` |
| `API_KEY` | OAuth 1.0a | Stockee dans Vault/Secrets Manager |
| `API_SECRET` | OAuth 1.0a | Stockee dans Vault/Secrets Manager |
| `ACCESS_TOKEN` | OAuth 1.0a (ecriture) | Variable d'environnement `X_ACCESS_TOKEN` |
| `ACCESS_TOKEN_SECRET` | OAuth 1.0a (ecriture) | Variable d'environnement `X_ACCESS_TOKEN_SECRET` |

### Etape 4 — Variables d'environnement (Docker)

```yaml
environment:
  X_BEARER_TOKEN: ${X_BEARER_TOKEN}
  X_API_KEY: ${X_API_KEY}
  X_API_SECRET: ${X_API_SECRET}
  X_ACCESS_TOKEN: ${X_ACCESS_TOKEN}
  X_ACCESS_TOKEN_SECRET: ${X_ACCESS_TOKEN_SECRET}
  KAFKA_BROKERS: broker-1:29092,broker-2:29092,broker-3:29092
```

---

## 8. Tableau de bord et monitoring

### 8.1 Metriques cles suivies (Prometheus/Grafana)

| Metrique | Seuil d'alerte | Action |
|---|---|---|
| kafka_broker_messages_in_per_sec | < 1 msg/s pendant 10 min | Verifier producer |
| consumer_lag{topic="tweets_raw"} | > 10 000 messages | Scale le consumer Spark |
| tweets_raw_producer_rejected_total | > 500/h | Ajuster les regles de recherche |
| up{job="kafka-broker"} | == 0 | Broker down -> alerte critique |

### 8.2 Dashboard Grafana — onglets prevus

1. **Overview** : volume total, sentiment global, top clubs
2. **Themes** : #VAR, #AnalyseVideo, #Stats, #PerformanceEquipe
3. **Consumer Lag** : sante du pipeline Kafka -> Spark
4. **Alerts** : historique des incidents

---

## 9. Risques et mitigations

| Risque | Probabilite | Impact | Mitigation |
|---|---|---|---|
| Changement de tarification par X | Moyenne | Haut | Surveillance mensuelle du pricing, alerte au depassement budget |
| Rate limits atteints | Faible | Moyen | Mise en cache, optimisation des requetes, backoff exponentiel |
| Brokers Kafka down | Faible | Haut | 3 brokers avec replication x3, alerting Slack automatique |
| Donnees biaisees (francophone uniquement) | Elevee | Moyen | Filtrer lang=fr volontairement — aligne avec le marche cible Ligue 1 |

---

## 10. Decision demandee

**Approbation sollicitee pour :**

- [ ] Souscription au plan **Basic tier (200 USD/mois)** ou activation du **pay-per-use** avec voucher
- [ ] Budget mensuel : **200-250 USD** (Basic + top-up occasionnel)
- [ ] Acces au compte X avec les permissions developpeur
- [ ] Integration des secrets API dans le vault de l'equipe

---

## Annexes

### A. Glossaire

| Terme | Definition |
|---|---|
| **Topic Kafka** | Canal de messagerie ou sont publies les evenements (tweets) |
| **Consumer Lag** | Retard de traitement d'un consumer par rapport aux messages produits |
| **Filtered Stream** | Endpoint API X qui permet de recevoir les tweets en temps reel selon des filtres |
| **JMX Exporter** | Agent Java qui expose les metriques de Kafka au format Prometheus |
| **NLP** | Natural Language Processing — traitement automatique du langage |

### B. Liens utiles

| Ressource | URL |
|---|---|
| X Developer Platform | https://developer.x.com |
| API X Pricing | https://docs.x.com/x-api/getting-started/pricing |
| API X Rate Limits | https://docs.x.com/x-api/fundamentals/rate-limits |
| Confluent Kafka Docs | https://docs.confluent.io |
| Prometheus Docs | https://prometheus.io/docs |
| Grafana Docs | https://grafana.com/docs |
