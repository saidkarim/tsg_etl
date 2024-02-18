## TSG ETL Tool - Take Home Challenge


### Getting Started
1. Setup a virtual environment (`pipenv`, `venv`, etc.)
2. Install requirements into your environment: `pip install -r requirements.txt`
3. Run ETL: `python main.py`. It's assumed that the api data source is already running at this stage.

### Tests
Run tests: `pytest tests`

### Implementation
The ETL process comprises three steps: `Extract`, `Transform`, and `Load`. 
Each step is implemented independently, except for `Load`. Due to the simplicity of the load step, 
it is not implemented separately. However, in practical applications, 
it would be beneficial to implement it separately, similar to the other steps. 
A common interface class named `ETL` orchestrates these steps.

| Step      | Implemented in                                   |
|-----------|--------------------------------------------------|
| Extract   | lib/extract.py                                   |
| Transform | lib/transform.py                                 |
| Load      | Not Implemented separately, exists in lib/etl.py |

