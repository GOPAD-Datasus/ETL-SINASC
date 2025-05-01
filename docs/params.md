# Params in ``parameters/params.json``

> ### **🛑Important:**
> 
> If years are outside the range $2012$ to $2023$, then additional
> years' download url must be added to ``get_urls()``, inside
> ``etl/src/extraction/utils.py``, and there must be a corresponding
> ``year_N.py`` inside ``etl/src/transformation/year_specific``
> containing a declaration of class ``HandlerN``

## ``starting_year``
- Initial year to pull data from (inclusive)
  - Ex. 2014 $\rightarrow$ Downloads DN2014.csv, DN2015.csv, ...
## ``ending_year``
- Final year to pull data from (also inclusive)
  - Ex. 2020 $\rightarrow$ Downloads ... DN2019.csv, DN2020.csv