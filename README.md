# Alert Detection System

Welcome to the Alerts Detection System (EAM Edition)! :alarm_clock: :chart_with_upwards_trend:

If you are tired of checking every day if your metrics have broken
or if they resulted in a value that does not correspond with your expectations, then you can use our
new Alert System to prevent that. :smile: You can configure multiple metrics from GA or BQ and
this system will notify you if any metric is not behaving well. And the best part... It is very simply to use!
We will explain it to you in a very brief way.

**Update**: Now with the Alert Detection System you are not only able to get the alerts, but you can also get
the predictions for the rest of the current month! If you want, you will receive an Excel archive with the future
predictions for every metric you introduce in the System.

## How to use the Alert Detection System (EAM Edition)

### Initial Setup

First of all, you have to download this repo to some folder in your local machine or clone it into a virtual one in
a new environment you create. The reason to use a environment is because the Alerts System uses a specific
Python version and some dependencies. You can create the environment as you prefer with the `requirements` folder
that is in the project. It contains a `environment.yml` (for *conda* envs) and `requirements.txt` (for *virtualenv* and others).
Then, for *conda* you can create the environment as follows:

```
conda env create -f requirements/environment.yml
conda activate alert_system
```

For *virtualenv* you can execute:

```
pip install virtualenv
virtualenv --python=/usr/bin/python3.9 alert_system
source alert_system/bin/activate
pip install -r requirements/requirements.txt
```

In order to use the Alerts Detection System, you must follow the steps in the *Initial Setup* section of this [docu](./AlertsDetectionSystem_Docu.pdf).

### Execute the Alert Detection System

To run the code, just open a terminal in the root folder of the project and execute the following command.

```
./execute_alerts.sh -e <environment>
```

You can introduce two different values in the *env* parameter: *dev* and *pro*, depending on you want to use each configuration file.

If you want the predictions for the rest of the current month, please execute:

```
./execute_alerts.sh -e <environment> -f True
```
