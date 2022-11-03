# Predicting NBA 2K player ratings using real life data


import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


import requests
import lxml.html as lh
import pandas as pd
import numpy as np


############################# Getting NBA player statistics #############################

def get_all_links(url, lst=[]):
    '''
    This funciton takes a url and returns a list of urls containing data for each NBA season

    Parameters
    ----------
    url : string
        link to a page on basketball reference website containing statitistics 
    lst : list, optional
        list that links to other seasons of data will be apended to. The default is [].

    Returns
    -------
    lst : list
        list of links for data for each NBA season
    '''
    
    page = requests.get(url)
    doc = lh.fromstring(page.content)
    links = doc.xpath('//div[@id="inner_nav"]//@href')
    
    try:
        nex = doc.xpath('//div[@class="prevnext"]/a[@class="button2 next"]/@href') #selecting URL for next year
        nex = 'https://basketball-reference.com' + nex[0]
    except:
        pass
    
    sub_str = ['total'] 
    
    ls = [i for i in links if any(sub in i for sub in sub_str)]
    
    string = 'https://basketball-reference.com' + ls[0]
    
    lst.append(string)
    
    if nex:
        get_all_links(nex, lst)
    
    return lst



def clean(data, year):
    '''
    This functions cleans a dataframe containing NBA player data by removing unwanted rows, changing data types,
    adding a new column for which 2k video game the data is for and othe minor adjustments

    Parameters
    ----------
    data : dataframe
        a dataframe containing a single season of NBA player statistics 
    year : int
        year corresponding to which 2k video game the data is for

    Returns
    -------
    merged : dataframe
        a cleaned version of the original data 
    '''
    
    data = data.drop_duplicates('Player', keep='first') #removing rows for duplciate players, first instance is there totals for the season
    data = data[data.Rk != 'Rk'] #removing rows that just contain the column names again
    
    #adjusting data types
    nonfloats = data[['Player', 'Pos', 'Tm']]
    ints = data[['Age', 'G', 'GS']]
    ints = ints.astype('int')
    floats = data.drop(columns=['Player', 'Pos', 'Tm', 'Age', 'G', 'GS'])
    floats = floats.astype('float')
    
    merged = pd.concat([nonfloats, ints, floats], axis=1)
    merged['Pos'] = merged['Pos'].astype('category')
    
    merged['2k'] = year #adding year so it can be joined with ratings data later
    merged = merged.replace(np.NaN, 0) #only missing values occur when player has 0 shots for given category, so just fill with 0
    
    merged.loc[merged['3P'] == 0, '3P%'] = 0
    
    merged = merged.drop(columns=['Rk', 'FG%', '3P%', '2P%', 'eFG%', 'FT%'])
        
    merged['Player'] = merged['Player'].map(lambda x: x.rstrip('*')).str.replace('.', '').str.replace("'e", "e")
    
    merged['Player'] = merged['Player'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    
    merged['Pos'] = merged['Pos'].str.split('-').str[0]
        
    return merged



def get_data(url, url2):
    '''
    This function gathers links for regular season and playoff NBA player statistics across multiple seasons, 
    scrapes each link for the required data and then cleans and combines all of the data

    Parameters
    ----------
    url : string
        url of the first regular season of NBA player statistics to gather data from
    url2 : TYPE
        url of the playoffs for the first NBA season to gather player statistics from

    Returns
    -------
    df : dataframe
        a cleaned dataframe containing NBA player statistics 
    '''

    links_reg = get_all_links(url)
    links_pla = get_all_links(url2, lst=[])
    links_reg = links_reg[:-1] #gets link for upcoming season, dont need it
    
    df = pd.DataFrame()
    
    year = 2010
    
    for(i, j) in zip(links_reg, links_pla):
        reg = pd.read_html(i)[0]
        reg = clean(reg, year)
        pla = pd.read_html(j)[0]
        pla = clean(pla, year)
        cols = reg[['Player', 'Pos', '2k', 'Age']]
        reg = reg.drop(columns=['Pos', 'Tm', 'Age', '2k'])
        pla = pla.drop(columns=['Pos', 'Tm', 'Age', '2k'])
        data = reg.set_index(['Player']).add(pla.set_index(['Player']), fill_value=0).reset_index()
        data2 = cols.merge(data)
        df = pd.concat((df, data2))
        year += 1
    
    df = df.reset_index(drop=True)
    
    return df



url  = 'https://www.basketball-reference.com/leagues/NBA_2009.html'  
url2 = 'https://www.basketball-reference.com/playoffs/NBA_2009.html'
  
data = get_data(url, url2)  

data.to_csv('basketball.csv', index=False)  
    


############################# Getting 2k Player Ratings #############################

twoK = 'https://hoopshype.com/nba2k/2009-2010/'
#2k10 since it is for 2009-2010 season, therefore using data from 08-09 season for ratings....

def cleanData(url, year):
    '''
    This function takes a URL containing NBA 2k player ratings and removes unwanted rows, renames the columns
    as well as creates a column labeling which game the ratings are from

    Parameters
    ----------
    url : string
        url to a page containing NBA 2k player ratings
    year : int
        year corresponding to the NBA 2k video game

    Returns
    -------
    data : dataframe
        a dataframe containing player name, 2k rating and which 2k game the rating is from
    '''
    
    data = pd.read_html(url)[0]
    data = data.drop('Unnamed: 0', axis=1)
    data.columns = ['Player', 'Rating']
    data['2k'] = year
    
    return data



def getRatings():
    '''
    This function scrapes player ratings for the NBA 2k video games and returns a dataframe containing the ratings

    Returns
    -------
    df : dataframe
        a dataframe containing player name, 2k rating and which 2k game the rating is from
    '''
    
    lst = []
    start = 2009 #2k 10 corresponds to 2009-2010 season
    current = 2023

    for i in range(current-start):
        lst.append('https://hoopshype.com/nba2k/{}-{}/'.format(start, start+1))
        start += 1

    df = pd.DataFrame()
    
    year=2010
    
    for l in lst:
        data = cleanData(l, year)
        df = pd.concat((df, data))
        year += 1
        
    return df



ratings = getRatings()

ratings.to_csv('ratings.csv', index=False)
