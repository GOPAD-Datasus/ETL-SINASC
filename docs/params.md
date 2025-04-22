# Params documentation
---
## Extraction related
### Source
> Where the files will come from
- ``opendatasus``: OpenDataSus website
- ``datasus``: DataSus website
  - If this is chosen, then files must be converted to csv first (may take longer)
### Starting and ending year
> Which interval to pull the data from
- How it works: [``start`, ``end``[
  - Ex. [``2012``, ``2013``[ -> only 2012
---
## Transformation related
### Structure
> Defines the structure of the data
- ``original``: (Default) Raw data from the source
- ``gopad``: If this option is chosen, then all the changes below will be applied
  - Same as manually setting ``yes`` for each item below 
### Drop System
> Each row has columns corresponding to the system used at the time of collection, this info is often not useful for analysis, so it can be safely removed

Those columns are: CONTADOR, ORIGEM, NUMEROLOTE, ,VERSAOSIST, 
DTRECEBIM, DIFDATA, DTCADASTRO
- ``yes``: removes system columns
- ``no``: (Default) doesn't remove system columns
### Drop Registry Office
> Each row has columns related to registry office, but not all rows go through it, leaving major info. gaps

Those columns are: CODCART, NUMREGCART, DTREGCART
- ``yes``: removes said columns
- ``no``: (Default) doesn't remove registry columns

