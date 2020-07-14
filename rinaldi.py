from kaggle.api.kaggle_api_extended import KaggleApi
import pandas
import os
import re
import numpy
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from IPython.display import display

api = KaggleApi()
api.authenticate()
    
global dataset_path    
dataset_path = str(input("Tap the complete path folder where you would like to save datasets:"))

print("The complete path folder you choosed is: {}; \nit is now stored within the rinaldi.py variable: dataset_path".format(dataset_path))




def data_search():
    """
    Parameters:     None.

    Returns:        dataset     : datasets;
                    files_list  : list.

    Description:    allows to search the desired dataset using Kaggle API by inserting
                    the required dataset keywords or url and returning datasets and its
                    files list details in order to be used as parameters to be downloaded
                    and saved with data_saving function.
    """
    datasets = api.dataset_list(search=str(input("Insert keywords or url of desired dataset"))) 
    dataset = datasets[0]
    owner_slug, dataset_slug = dataset.ref.split("/")
    response = api.datasets_list_files(owner_slug=owner_slug, dataset_slug=dataset_slug)
    errorMessage, files_list = response["errorMessage"], response["datasetFiles"]
    if errorMessage is not None:
        raise RuntimeError("Something went wrong during dataset recovering!")
    for file in files_list:
        print("Datasets found: {}".format(file['name']))
    return dataset, files_list;




def data_saving(data_search, path : str = dataset_path):
    """
    Parameters:     data_search : datasets, mandatory, positional;
                    path        : string, mandatory, positional, default to dataset_path
                                  variable user is asked to declare at the program start.

    Returns:        list.

    Description:    allows to save the datasets found with data_search function within a
                    folder whose default path is the declared path by the user at startup, 
                    by renaming datasets with more suitable names for user work and to save
                    names within a list.
    """
    api.dataset_download_files(data_search[0].ref, path = dataset_path, unzip=True)
    files_name = []
    for i, file in enumerate(data_search[1]):
        try:
            new_name = input("Insert the names with whom it is desired to save datasets in the format name.extension").replace(" ","")
            files_name.append(new_name)
            old_name = os.path.join(dataset_path, file['name'])
            new_name = os.path.join(dataset_path, new_name)
            os.rename(old_name, new_name)
        except:
            os.remove(os.path.join(dataset_path, file['name']))
            raise ValueError("Error: impossible rename file {}".format(file['name'])) 
        else:
            print("Successfully renamed file: {}, in: {}".format(file['name'], str(files_name).replace("['","").replace("']","")))
    return files_name




def data_reading(name : str, path = dataset_path):
    """
    Parameters:     name : string, mandatory, positional.
                    path : string, mandatory, positional, default to dataset_path
                           variable user is asked to declare at the program start.

    Returns:        DataFrame.

    Description:    allows read the datasets saved with data_saving function between
                    .csv, .json or .xls. extension by passing as arguments the name
                    of the file (as string without .ext) and the path whose default
                    value is the declared dataset_path by the user at startup.
    """
    ext = {".csv": pandas.read_csv, ".json": pandas.read_json, ".xls": pandas.read_excel}
    for e, f in ext.items():
        file_name = name + e
        if os.path.isfile(os.path.join(path, file_name)):
            d_read = f(os.path.join(path, file_name))
            return d_read
        else:
            raise ValueError("File name: {} does not exist or file path: {} is wrong".format(name, path))




def settings(analysis_storage_file_name : str = 'analyses_results', analysis_storage_path: str = dataset_path):
    """
    Parameters:     name : string, mandatory, positional, default to 'analyses_results';
                    path : string, mandatory, positional default to dataset_path
                           variable user is asked to declare at the program start.

    Returns:        None.

    Description:    allows to create a file within the local module path to be considered
                    as a custom settings storage file for data analysis results saving tool.
    """
    analysis_storage_file_name = analysis_storage_file_name + ".txt"
    try:
        settings_name = "configuration.txt"
        with open(settings_name, 'w') as conf:
            conf.write("{}\n{}".format(analysis_storage_path, analysis_storage_file_name))
            conf.close()
    except:
        print("Error: mandatory parameters have to be strings !!!") 
    else:
        print("Successfully created settings file: {}".format(settings_name))




class NoMatchFound(ValueError):
    pass


def subset_genesis(data_reading, year : str = None, column_name : str = 'tournament', row_name : str = "FIFA World Cup"):
    """
    Parameters:     data_reading : DataFrame, mandatory, positional.
                    year         : string, mandatory, positional, default to "None".
                    column_name  : string, mandatory positional, default to "tournament".
                    row_name     : string, mandatory positional, default to "FIFA World Cup". 

    Returns:        DataFrame.

    Description:    allows to create two possible subset kinds from the original data_reading DataFrame:
                       1. if year is kept equal to None, subset is grouped by column_name and got by row_name group;
                       2. if year is not equal to None, subset is filtered by index year parameter, grouped by the
                          desired column_name and got by row_name group.
                    Default value of column_name is recommended to be setted equal to 'tournament' because of
                    enabling following subset_analysis results; nevertheless, different groupby standards can
                    be used (for instance, 'home_team', 'away_team') even though following subset_analysis will
                    be not enabled.

    """
    ddrr = data_reading.copy()
    a = pandas.to_datetime(ddrr[ddrr.columns[0]])
    ddrr[ddrr.columns[0]] = a
    b = pandas.to_numeric(ddrr[ddrr.columns[3]])
    ddrr[ddrr.columns[3]] = b
    c = pandas.to_numeric(ddrr[ddrr.columns[4]])
    ddrr[ddrr.columns[4]] = c
    ddrr.set_index(ddrr.columns[0], inplace = True)
    try:
        ddrr_subset = ddrr[year] if year != None and year in ddrr.groupby(column_name).get_group(row_name).index else ddrr
        return ddrr_subset.groupby(column_name).get_group(row_name)
    except:
        raise NoMatchFound("No matches were played in {} for {} tournament".format(year,row_name))




def subset_analysis(subset_genesis):
    """
    Parameters:     subset_genesis : DataFrame, mandatory positional.

    Returns:        dict.

    Description:    allows to perform analyses on the subset_genesis DataFrame subset passed as argument
                    by returning the following five aggregation values in the form of a dictionary:
                        1. tournament's scored goals mean (int);
                        2. tournament's team maximum scored goals (tuple of team name and max goals);
                        3. tournament's city where maximum scored goals (tuple of city name and max goals);
                        4. tournament's team minimum scored goals (tuple of team name and min goals);
                        5. tournament's city where minimum scored goals (tuple of city name and min goals).
    """
    df = subset_genesis
    df["goals"] = df.iloc[:,2] + df.iloc[:,3]
    if len(df[df.columns[4]].unique()) == 1:
        team_max_score = pandas.DataFrame(df.groupby(df.columns[0])[df.columns[2]].sum().add(df.groupby(df.columns[1])[df.columns[3]].sum(),fill_value = 0)).sort_values(0, ascending = False)
        city_max_score = df.assign(total = df[df.columns[2]] + df[df.columns[3]]).groupby(df.columns[5]).sum().sort_values("total", ascending = False)
        team_min_score = pandas.DataFrame(df.groupby(df.columns[0])[df.columns[2]].sum().add(df.groupby(df.columns[1])[df.columns[3]].sum(),fill_value = 0)).sort_values(0, ascending = True)
        city_min_score = df.assign(total = df[df.columns[2]] + df[df.columns[3]]).groupby(df.columns[5]).sum().sort_values("total", ascending = True)

        l_two = [team_max_score.index[i] for i in range(len(team_max_score.head(20))) if team_max_score.iloc[i,0] == team_max_score.iloc[0, 0]]
        l_three = [city_max_score.index[i] for i in range(len(city_max_score.head(20))) if city_max_score.iloc[i,0] == city_max_score.iloc[0, 0]]
        l_four = [team_min_score.index[i] for i in range(len(team_min_score.head(20))) if team_min_score.iloc[i,0] == team_min_score.iloc[0, 0]]
        l_five = [city_min_score.index[i] for i in range(len(city_min_score.head(20))) if city_min_score.iloc[i,0] == city_min_score.iloc[0, 0]]

        l_two = "-".join(l_two)
        l_three = "-".join(l_three)
        l_four = "-".join(l_four)
        l_five = "-".join(l_five)

        aggregation_value_one = df.mean()[3].round(3)
        aggregation_value_two = str(l_two).replace("[","").replace("]","").replace("'",""), team_max_score.iloc[0,0]
        aggregation_value_three = str(l_three).replace("[","").replace("]","").replace("'",""), city_max_score.iloc[0,0]
        aggregation_value_four = str(l_four).replace("[","").replace("]","").replace("'",""), team_min_score.iloc[0,0]
        aggregation_value_five = str(l_five).replace("[","").replace("]","").replace("'",""), city_min_score.iloc[0,0]

        result = {"scored goals mean": aggregation_value_one, "max team scored goals": aggregation_value_two, "max city scored goals": aggregation_value_three, "min team scored goals": aggregation_value_four, "min city scored goals": aggregation_value_five}
        return result
    else:
        raise NoMatchFound("Subset_genesis standards do not meet subset_analysis requirements. Please, set '{}' as groupby argument within subset_genesis function".format(df.columns[4]))




class ConfigurationParametersAbsent(ValueError):
    pass


def results_saving(subset_analysis):
    """
    Parameters:     subset_analysis : dict, mandatory, positional.

    Returns:        None.

    Description:    allows to store the subset performed analyses on a storage file, associating each entry
                    with an identifier (analysis date and time). Needs to read settings parameters in order
                    to save analyses results within the file to create: if it is not allowed, exception will
                    be raised. Cases because of exception can happen are:
                        1. settings file does not exist;
                        2. settings function it has not ever been initialized 
    """
    now = datetime.datetime.now()
    ID = str(now.year) + str(now.month) + str(now.day) + str(now.hour) + str(now.minute) + str(now.second)
    settings_name = "configuration.txt"
    dic_res = str(dict.values(subset_analysis)).replace("dict_values([","").replace(" ","")
    dic_res = re.sub(r"[\[[\](')]", '', dic_res)
    if os.path.isfile(settings_name) == True:
        with open((settings_name), 'r') as conf:
            configuration = conf.readlines() 
            conf.close()
        conf_path = configuration[0].replace("\n","")
        conf_name = configuration[1]
        if os.path.isfile(os.path.join(conf_path, conf_name)) == True:
            with open(os.path.join(conf_path, conf_name), 'a') as res:
                res.write(ID +"," + dic_res + "\n")
            with open(os.path.join(conf_path, conf_name), 'r+') as res:
                res.write("id,mean,max_scored_goals_team,max_team_goals,max_scored_goals_city,max_city_goals,min_scored_goals_team,min_team_goals,min_scored_goals_city,min_city_goals"+"\n")
                res.close()
        else:
            with open(os.path.join(conf_path, conf_name),'w'):
                with open(os.path.join(conf_path, conf_name), 'r+') as res:
                    res.write("id,mean,max_scored_goals_team,max_team_goals,max_scored_goals_city,max_city_goals,min_scored_goals_team,min_team_goals,min_scored_goals_city,min_city_goals"+"\n")
                with open(os.path.join(conf_path, conf_name),"a") as res:
                    res.write(ID +"," + dic_res + "\n")
                    res.close()
        print("Correctly saved analysis results in {} file".format(conf_name))
    else:
        raise ConfigurationParametersAbsent("Settings file to save analysis results parameters does not exist!!!\nMake sure you have created it by using settings function")




def plot_genesis(subset_genesis, name: str = "KDE"):
    """
    Parameters:     subset_genesis : DataFrame, mandatory, positional;
                    name           : string, mandatory, positional, default to "None".

    Returns:        fig            : Figure.

    Description:    allows to plot kernel density estimation plot - by choosing the desired name
                    passed as parameter - for home teams scored goals and away teams scored goals
                    in relation to the selected subset passed as parameter. If name is not given,
                    a standard one will be assigned.
    """
    df = subset_genesis
    plt.figure(figsize=(18,6))
    ax = df[df.columns[2]][:].plot.kde()
    ax = df[df.columns[3]][:].plot.kde()
    ax.set_title("{} scored goals".format(name), fontsize=30, fontweight = 200) 
    ax.legend()
    settings_name = "configuration.txt"
    try:
        if os.path.isfile(settings_name) == True:
            with open((settings_name), 'r') as conf:
                configuration = conf.readlines() 
                conf.close()
            conf_path = configuration[0].replace("\n","")
        if name == "KDE":
            now = datetime.datetime.now()
            name = str(now.day) + str(now.hour) + str(now.minute) + str(now.second)
        plt.savefig("{}\{}.png".format(conf_path, name))
    except:
        raise ConfigurationParametersAbsent("Settings file to save analysis results parameters does not exist!!!\nMake sure you have created it by using settings function")
    return plt.show()




def subplots_genesis(data_reading, name: str = None, column_name: str = "tournament", row_name: str = "FIFA World Cup"):
    """
    Parameters:     data_reading : DataFrame, mandatory positional.
                    name         : string, mandatory, positional, default to "None". 
                    column_name  : string, mandatory, positional, defaults to "tournament"
                    row_name     : string, mandatory, positional, defaults to "FIFA World Cup" 
 
    Returns:        fig          : Figure;
                    subplots     : array of axes.

    Description:    allows to plot horizontal bar charts - by choosing the desired name passed as
                    parameter - where on the x label total teams' scored goals for a given column_name
                    are displayed and on the y label teams which played matches within that competition
                    kind that year. If name is not given, a standard one will be assigned.

                 1. If column_name and row_name parameters are equals to default, plot_genesis will 
                    return n horizontal bar charts for each FIFA World Cup on that n-th year.

                 2. If column_name and row_name parameters are equals to None type, plot_genesis will 
                    return n horizontal bar charts for each played match within the n-th year.
    """
    df = data_reading.copy()
    d = df.groupby(column_name).get_group(row_name) if column_name != None and row_name != None else df
    col_position = d.columns.get_loc(df.columns[0])
    insert_df = d.pop(df.columns[0]).str.split("-",expand=True).set_axis(['year','month','day'],axis='columns')
    df_by_positions = ([d.iloc[:,:col_position],insert_df,d.iloc[:,col_position:]])
    d_new = pandas.concat(df_by_positions, axis=1)
    years = d_new.year.unique()
    ncols = 3
    div, rem = divmod(len(years), ncols)
    nrows = div + 1 if rem else div
    fig, subplots = plt.subplots(nrows = nrows, ncols=ncols, figsize = (50, 80))
    for y, ax in zip(years, subplots.reshape(-1)):
        sub_df = d_new[d_new.year == y]
        all_teams = sub_df.home_team.unique()
        df_goals = sub_df.groupby(sub_df.columns[3])[sub_df.columns[5]].sum().add(sub_df.groupby(sub_df.columns[4])[sub_df.columns[6]].sum())
        df_goals = df_goals.reindex(all_teams).sort_index().fillna(value = 0)    
        rects = ax.barh(df_goals.index, df_goals)
        line = ax.axvline(df_goals.mean(), color = "gray")
        ax.set_title(pandas.to_datetime(y).strftime("%Y"), fontsize=24)
    plt.suptitle("{} teams' scored goals".format(name), y = -0.01, fontsize=36, fontweight = 400) 
    plt.rc('xtick', labelsize = 22)
    plt.rc('ytick', labelsize = 22)
    plt.tight_layout(pad = 0.5, w_pad = 0.6, h_pad = 2.0)
    settings_name = "configuration.txt"
    try:
        if os.path.isfile(settings_name) == True:
            with open((settings_name), 'r') as conf:
                configuration = conf.readlines() 
                conf.close()
            conf_path = configuration[0].replace("\n","")
        if name == None:
            now = datetime.datetime.now()
            name = "HBC" + str(now.day) + str(now.hour) + str(now.minute) + str(now.second)
        plt.savefig("{}\{}.png".format(conf_path, name))
    except:
        raise ConfigurationParametersAbsent("Settings file to save analysis results parameters does not exist!!!\nMake sure you have created it by using settings function")
    return plt.show()
