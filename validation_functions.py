import os
import re
import pandas as pd
import numpy as np

SUBMISSION_PATTERN = re.compile(r"forecasts/submissions.*/(.+)/(.+)/(.+)/\d\d\d\d-\d\d-\d\d-\1-\2-\3.csv")

VALID_COLUMNS = ['location', 'age_group', 'forecast_date', 'target_end_date', 'horizon', 
                 'type', 'quantile', 'value']

LOCATION_CODES = ['DE', 'DE-BB-BE', 'DE-BW', 'DE-BY', 'DE-HE', 'DE-MV', 'DE-NI-HB',
                  'DE-NW', 'DE-RP-SL', 'DE-SH-HH', 'DE-SN', 'DE-ST', 'DE-TH']

VALID_QUANTILES = [0.025, 0.1, 0.25, 0.5, 0.75, 0.9, 0.975]
VALID_TYPES = ['mean', 'quantile']
VALID_AGE_GROUPS = ['00+', '00-04', '05-14', '15-34', '35-59', '60-79', '80+', '60+']
VALID_HORIZONS = [-3, -2, -1, 0, 1, 2, 3, 4]

def check_filepath(filepath):
    if SUBMISSION_PATTERN.match(filepath) == None:
        return "The file does not follow the naming convention for submissions or is located in the wrong directory."
      
def check_forecast_date(filepath):
    try:
        file_forecast_date = pd.to_datetime(os.path.basename(filepath)[:10]).date()
    except:
        return f"Date of filename in wrong format: {os.path.basename(filepath)[:10]}. Should be yyyy-mm-dd."
    
    df = pd.read_csv(filepath)    

    if df.forecast_date.nunique() > 1:
        return f"The file contains multiple forecast dates: {df.forecast_date.unique()}. Forecast date must be unique." 
    
    try:
        column_forecast_date = pd.to_datetime(df.forecast_date.iloc[0]).date()
    except:
        return f"Date in column \'forecast_date\' in wrong format: {df.forecast_date.iloc[0]}. Should be yyyy-mm-dd."

    if file_forecast_date != column_forecast_date:
        return f"Date of filename {os.path.basename(filepath)} does not match column \'forecast_date\': {column_forecast_date}." 

    # if pd.to_datetime(os.path.basename(filepath)[:10]).day_name() != 'Thursday':
    #     return f"The forecast date is a {file_forecast_date.day_name()}. It should be a Thursday."

    today = pd.Timestamp('today', tz='Europe/Berlin').date()
    if ('retrospective' not in filepath) and (abs(file_forecast_date - today).days > 1):
        return f"The forecast is not made today. Date of the forecast: {file_forecast_date}, today: {today}."

def check_column_values(df):
    invalid_values = dict()
    invalid_values['location'] = [_ for _ in df.location.unique() if _ not in LOCATION_CODES]
    invalid_values['quantile'] = [_ for _ in df['quantile'].dropna().unique() if _ not in VALID_QUANTILES]
    invalid_values['type'] = [_ for _ in df.type.unique() if _ not in VALID_TYPES]
    invalid_values['age_group'] = [_ for _ in df.age_group.unique() if _ not in VALID_AGE_GROUPS]
    invalid_values['horizon'] = [_ for _ in df.horizon.unique() if _ not in VALID_HORIZONS]
    
    errors = []
    for key, value in invalid_values.items():
        if len(value) > 0:
            errors.append(f'Invalid entries in column \'{key}\': {value}')
    
    if len(errors) > 0:
        return errors

def check_header(df):
    missing_cols = [c for c in VALID_COLUMNS if c not in df.columns]
    additional_cols = [c for c in df.columns if c not in VALID_COLUMNS]
    
    errors=[]
    
    if len(missing_cols) > 0:
        errors.append(f'The following columns are missing: {missing_cols}. Please add them.')
    
    if len(additional_cols) > 0:
        errors.append(f'The following columns are not accepted: {additional_cols}. Please remove them.')
        
    if len(errors) > 0:
        return errors

def check_target_dates(df):
    df['invalid_target_date'] = df.apply(lambda x: x.target_end_date != x.forecast_date + 
                                         pd.Timedelta(weeks = x.horizon, days = -4), axis = 1)
    
    invalid_target_dates = df.loc[df.invalid_target_date, ['forecast_date', 'target_end_date', 'horizon']].drop_duplicates()
    if len(invalid_target_dates) > 0:
        error = 'The following target_end_dates are wrong:\n\n' + invalid_target_dates.to_string(index = False)
        return error

def check_value(df):
    errors = []
    if df.value.isnull().sum():
        errors.append(f'Missing values in column \'value\' are not allowed. {df.value.isnull().sum()} values are missing.')
    
    non_numeric_values = df.value[~df.value.astype(str).str.replace('.', '', regex = False).str.isnumeric()].dropna().to_list()
    if len(non_numeric_values) > 0:
        errors.append(f'Non-numeric entries in column \'value\' are not allowed: {non_numeric_values}.')
    
    if len(errors) > 0:
        return errors
    
def check_mean(df):
    n = df[df.type == 'mean']['quantile'].notnull().sum()
    
    if n > 0:
        error = f"Rows with type \"mean\" should have NA in column \'quantile\'. This was violated {n} time{'s' if (n > 1) else ''}."
        return error
    
def check_duplicates(df):
    df_duplicated = df[df.duplicated(subset = ['location', 'age_group', 'forecast_date', 'target_end_date', 
                           'horizon', 'type', 'quantile'], keep = False)].copy()
    
    df_duplicated.sort_values(['location', 'age_group', 'forecast_date', 'target_end_date', 
                           'horizon', 'type', 'quantile'], inplace = True)
    
    n = len(df_duplicated)
    
    if n > 0:
        error = f"Duplicated targets present. Check the following {n} rows. \n\n {df_duplicated.to_string()}"
        return error

def check_quantiles(df):
    df.loc[df.type != 'mean', 'no_quantiles'] = df[df.type != 'mean'].groupby(['location', 'age_group', 'horizon', 
                                                                           'target_end_date'])['quantile'].transform('nunique')
    
    # note that we've already checked that no invalid quantiles are present
    incomplete_quantiles = df[(df.no_quantiles != 7) & df.no_quantiles.notnull()]
    
    only_mean = all(df.type == 'mean')
    only_median = all(df.loc[df.type == 'quantile', 'quantile'] == 0.5) # currently allows mean and median forecasts
    
    if (len(incomplete_quantiles) > 0) and not (only_mean or only_median):
        error = 'Not all quantiles were provided in the following setting(s):\n\n' + \
            incomplete_quantiles.groupby(['location', 'age_group', 'horizon', 'target_end_date']
                                        )['quantile'].unique().to_string()
        return error
    
def check_forecast(filepath):
    errors = []

    result = check_filepath(filepath)
    if result:
        errors.append(result)
      
    result = check_forecast_date(filepath)
    if result:
        errors.extend(result if isinstance(result, list) else [result])
    
    df = pd.read_csv(filepath, parse_dates = ['forecast_date', 'target_end_date'])
    
    for check in [check_header, check_column_values, check_value, check_mean, check_duplicates, check_target_dates, check_quantiles]:
        try:
            result = check(df)
            if result:
                errors.extend(result if isinstance(result, list) else [result])
        except:
            errors.append(f"Fatal error: {check.__name__} could not be completed.")

    return errors
