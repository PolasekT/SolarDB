# SolarDB

Python API for the SolarDB photovoltaic dataset.

## What is This?

This repository contains auxiliary code for accessing the SolarDB dataset. 
The SolarDB is a photovoltaic dataset consisting of 1-year of data for 16 
solar power plants, including power production, weather, weather forecasts, 
and additional exogenous features. In total, it contains over 40M records.

The SolarDB dataset is a part of a currently in-review paper. For more 
information, see [cphoto.fit.vutbr.cz/solar](http://cphoto.fit.vutbr.cz/solar): 

## How to Install It?

The `solardb` package can be simply installed by following the subsequent 
instructions. First, `solardb` has the following dependencies: 
 * Python >= 3.7.1
 * numpy
 * pandas
 * scipy
 * scikit-learn
 * sqlalchemy

Next, to install the `solardb` package and any dependencies, run the following: 
```
conda create --name solardb -y python">=3.7.1" numpy pandas \
    scipy scikit-learn sqlalchemy
conda activate solardb
git clone https://github.com/PolasekT/SolarDB ./solardb
cd ./solardb && python ./setup.py install
```

## How to Use It?

The `solardb` package only contains the code necessary for access to the 
various data sources. First, download the *db* files from the projects 
[website](http://cphoto.fit.vutbr.cz/solar). Next, to test if the installation 
is valid, run the following Python commands: 
```
from solardb.db import SolarDBData

db = SolarDBData("./path/to/solardb.db")

print(f"Total power plant / inverters: {len(db.get_pp_info_df())}")
% Output: Total power plant / inverters: 274
```

For a quick-start, the `SolarDBAssembler` and `SolarDBEvaluator` may be used. 
First, we calculate history, weather, and prediction DataFrames for the prediction 
task of `<10.5.2019, 11.5.2019)` for power plant #8: 
```
import datetime
from solardb.db import SolarDBAssembler

assembler = SolarDBAssembler(db)

history_df, weather_df, prediction_df = assembler.prepare_prediction(
    pp_id=8,
    dt_start=datetime.datetime(2019, 5, 10),
    dt_end=datetime.datetime(2019, 5, 11),
    weather_scheme="realistic", history_cnt=288,
)

print(f"DataFrame lengths: {len(history_df)}, {len(weather_df)}, {len(prediction_df)}")
# Output: DataFrame lengths: 288, 288, 288
```

Next, we calculate the predictions, filling the `prediction_df` DataFrame. In 
this example, our model just it with a constant value: 
```
prediction_df.fillna(value=42, inplace=True)
```

Finally, we can evaluate the efficacy of the proposed prediction model: 
```
from solardb.db import SolarDBEvaluator

evaluator = SolarDBEvaluator(db)

evaluation = evaluator.evaluate_prediction(
    prediction_df=prediction_df,
    history_df=history_df,
    weather_df=weather_df
)

print(f"Prediction error = {evaluation['power']['error'] * 100.0:.2f}%")
# Output: Prediction error = 99.14%
```

For additional uses, see code documentation in `SolarDBData` and the helper script 
`solardb/run/solardb_main.py`.

## License

The code in this repository is licensed under MIT. For information about the 
SolarDB licensing, see the project [website](http://cphoto.fit.vutbr.cz/solar). 

