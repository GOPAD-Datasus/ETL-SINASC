# ETL Project: SINASC

A simple ETL (Extract, Transform, Load) pipeline to facilitate the use of
[DataSUS](https://datasus.saude.gov.br/transferencia-de-arquivos/)'s SINASC database.
With focus on the files corresponding to the years 2012 to 2023.

## 📌 Overview
- **Extract**: Data sourced from [OpenDataSUS](https://opendatasus.saude.gov.br/), and stored in ``temp/raw`` folder with .csv format.
- **Transform**: Every year goes through year specific changes, having its columns standardized and dtypes optimized. At the end, they
are stored inside ``temp/processed``
- **Load**: Load transformed data on to Postgres. Alternatively, files created can be accessed at [DB-SINASC](https://github.com/GOPAD-Datasus/DB_SINASC)

## 🚀 Setup
1. Clone the repo:
   ```bash
   git clone https://github.com/GOPAD-Datasus/ETL-SINASC
   ```
2. Install dependencies
   ```bash
   poetry install
   ```
   - Note: Poetry must be installed for the command above to work

## ⚙ Run
- If your IDE supports in project virtual environments (like PyCharm), use:
```bash
python main.py
```
- Else, add ``poetry run`` to the beginning of the command:
```bash
poetry run python main.py
```

## 🔮 Future Features
- Small changes planned:
  - Structural validation checks
  - Improved error handling 
- Better tests

## ✨ Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you'd like to change.

## 📝 License
[LGNU](LICENSE) | © GOPAD 2025