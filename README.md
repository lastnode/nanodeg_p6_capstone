# Introduction

A part of the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027), this [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) project was developed as the DEND's Capstone Project.

It pulls chess game data from two popular chess website APIs:

1) [The Lichess.org API](https://lichess.org/api)
2) [The Chess.com API](https://www.chess.com/news/view/published-data-api) — via the `chessdotcom` [Python module](https://pypi.org/project/chess.com/)

— and then performs ETL operations on these data using [Apache Spark](https://spark.apache.org/) ([PySpark](https://spark.apache.org/docs/latest/api/python/index.html)), moving these 1M+ rows from a collection of raw API responses to finally rendering them as aggregate fact tables that display useful information regarding chess openings and players found in the dataset.

During this process, data is saved at each stage in [Apache Parquet](https://parquet.apache.org/) format, as it has [a number of advantages over other file formats such as CSV](https://blog.openbridge.com/how-to-be-a-hero-with-powerful-parquet-google-and-amazon-f2ae0f35ee04), including being a columnar storage format. Throughout this process, data can be saved both locally, or to [Amazon S3](https://aws.amazon.com/s3/).

# Data Model

## Lichess API JSON 
The Lichess API endpoint we are using to get games (`https://lichess.org/api/games/user/{username}`) returns a JSON blob that looks like this:

```
{
"id": "q7ZvsdUF",
"rated": true,
"variant": "standard",
"speed": "blitz",
"perf": "blitz",
"createdAt": 1514505150384,
"lastMoveAt": 1514505592843,
"status": "draw",
"players": {
"white": {},
"black": {}
},
"opening": {
"eco": "D31",
"name": "Semi-Slav Defense: Marshall Gambit",
"ply": 7
},
"moves": "d4 d5 c4 c6 Nc3 e6 e4 Nd7 exd5 cxd5 cxd5 exd5 Nxd5 Nb6 Bb5+ Bd7 Qe2+ Ne7 Nxb6 Qxb6 Bxd7+ Kxd7 Nf3 Qa6 Ne5+ Ke8 Qf3 f6 Nd3 Qc6 Qe2 Kf7 O-O Kg8 Bd2 Re8 Rac1 Nf5 Be3 Qe6 Rfe1 g6 b3 Bd6 Qd2 Kf7 Bf4 Qd7 Bxd6 Nxd6 Nc5 Rxe1+ Rxe1 Qc6 f3 Re8 Rxe8 Nxe8 Kf2 Nc7 Qb4 b6 Qc4+ Nd5 Nd3 Qe6 Nb4 Ne7 Qxe6+ Kxe6 Ke3 Kd6 g3 h6 Kd3 h5 Nc2 Kd5 a3 Nc6 Ne3+ Kd6 h4 Nd8 g4 Ne6 Ke4 Ng7 Nc4+ Ke6 d5+ Kd7 a4 g5 gxh5 Nxh5 hxg5 fxg5 Kf5 Nf4 Ne3 Nh3 Kg4 Ng1 Nc4 Kc7 Nd2 Kd6 Kxg5 Kxd5 f4 Nh3+ Kg4 Nf2+ Kf3 Nd3 Ke3 Nc5 Kf3 Ke6 Ke3 Kf5 Kd4 Ne6+ Kc4",
"clock": {
"initial": 300,
"increment": 3,
"totalTime": 420
}
}
```

## Chess.com API JSON 

The Lichess API endpoint we are using to get games (`https://api.chess.com/pub/player/{username}/games/{YYYY}/{MM}`) returns a JSON blob that looks like this:

```
{
  "white": { // details of the white-piece player:
    "username": "string", // the username
    "rating": 1492, // the player's rating at the start of the game
    "result": "string", // see "Game results codes" section
    "@id": "string", // URL of this player's profile
  },
  "black": { // details of the black-piece player:
    "username": "string", // the username
    "rating": 1942, // the player's rating at the start of the game
    "result": "string", // see "Game results codes" section
    "@id": "string", // URL of this player's profile
  },
  "url": "string", // URL of this game
  "fen": "string", // final FEN
  "pgn": "string", // final PGN
  "start_time": 1254438881, // timestamp of the game start (Daily Chess only)
  "end_time": 1254670734, // timestamp of the game end
  "time_control": "string", // PGN-compliant time control
  "rules": "string", // game variant information (e.g., "chess960")
  "eco": "string", //URL pointing to ECO opening (if available),
  "tournament": "string", //URL pointing to tournament (if available),  
  "match": "string", //URL pointing to team match (if available)  
}
```

These JSON blobs are flattened and then stored as `*.parquet` files in the `raw/` directory.


# Files
```
- README.md -- this file
- etl-api-lichess.py -- script that fetches data from the Lichess API
- etl-api-chessdotcom.py -- script that fetches data from the Chess.com API
- etl-staging.py -- script that takes the raw API data and outputs a staging table for each chess site
- etl-data-quality.ipynb -- Jupyter notebook that unions the two staging tables and performs data quality checks
- analytics.ipynb -- Jupyter notebook that runs the analytics queries and outputs the aggregate fact tables
```