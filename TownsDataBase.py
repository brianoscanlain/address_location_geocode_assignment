#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
~ TownsDataBase Library v1.0 ~
------------------------------
TownsDataBase provides functionality to the user for matching Country Names, 
Town Names to a particular query. TownDataBase relies on a large dataset of 
known & accepted values. The dataset is largely mined from GeoNames and 
WikiPedia web sources. In addition, poorly documented country aliases are added 
manually, and can be updated by the user at any time. Queries are compared with 
values in this large dataset and the closest-matching value is returned.

The TownDataBase module is intended to be used as follows:
PREREQ: Setup a local compilation of the dataset [perform once]. This can be 
        achieved by scraping all the data from the web [can take ~65 minutes], or
        an archive of the data can be downloaded swiftly.
       
    1.  Load the local dataset into memory (~26 MB).
    
    2.  Evaluate queries using the query* functions.

Functions:
----------
reCompileDB():    
    >description    Compiles the large dataset from online sources, or 
                    alternatively,from a zipped repository (to be implemented).
    >inputs         PATH - Where to save the dataset
                    Scrape - type Boolean True|False. If false, it downloads a
                    precompiled dataset from an online repository. If True, the
                    Data is scraped from the original sources (GeoNames, Wiki).  
    >output         Saves datafiles locally on host system.

loadDB():
    >description    Loads the local dataset into memory as an object type DICT. 
                    Assign a name to this object, e.g. 
                        townsDB = TownsDataBase.loadDB()
    >inputs         PATH - Where local dataset is saved.
    >output         object dict containing standardised information on all known
                    countries, provinces, counties, cities and towns. Additional
                    information on these places, such as Latitude,Longitude, 
                    population (appropriate to circa 2012), area and class of 
                    town. 

queryCountryName(): 
    >description    Matching of query to dataset of known values. Matches are 
                    evaluated using a liklihood metric, and if it is above a    
                    specified threshold, the best match will be returned.
    >inputs         CountryEntry - query of type STR 
                    TownsDB - var name of dataset loaded in memory
                    Threshold - threshold of matching value: [0.0 -- 1.0]
                    LowestAllowedThreshold - Specifc last resort threshold value
                                             which allows the best match to be 
                                             returned after a thorough matching
                                             process [it can take ~10 seconds]
    >ouput          returns list containing the two-char country ID, and the 
                    liklihood metric of the matching process [0.0 --> 1.0].
                    
                    
                
                
Example: (assuming you have ran recompileDB() )
--------                         
>> import TownsDataBase
>> townsDB=TownsDataBase.loadDB()    #load the dataset into memory as townsDB obj
>> TownsDataBase.queryCountryName('Ireland',townsDB)
['IE',1.0]       #returns the 2-char country ID and the likelihood metric [0.0-->1.0]
>> TownsDataBase.queryCountryName('Britin',townsDB)
[None,0.0]       #checker fails to return, so two options, reduce thresholds or
                 #or add an alias to the alias list, then recompile.
                
                
Improving usage:
----------------   
I suggest using relatively high threshold values 0.65 -- 0.95, and occasionally
updating the local dataset (using the recompileDB).                


Version Control:
---------------
v1.0 Initial writing and testing. 09-Apr-2019. Brian Scanlon


ToDoList:
---------
1. Write up a function to query town/city. It should return townIndex (primary key)
   so that Latitude, lngitude, population of town/city can be easily found at a
   later date.


"""

import json, glob, difflib, os
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from fuzzywuzzy import process
#import requests
import zipfile
import numpy as np

DelimAliases = re.compile(r";|,| - ")
coding='lxml'  #coding='html5lib'\

#Preamble / definitions:
dataBaseParentPath = './DataBaseLocal/CountryInfo/'
#view archive compiled zip on goodle drive:
#https://drive.google.com/file/d/1MXBuvFwDXqEm9hqP0llDilIqpU_N-2oS/view?usp=sharing
ArchiveUrl = 'https://drive.google.com/open?id=1nd2yS9HTeGqdcMUvz35WQ1p9QF13tWVj'
ArchiveID = '1nd2yS9HTeGqdcMUvz35WQ1p9QF13tWVj'

def loadDB(PATH =dataBaseParentPath):
    #==============================================================================
    #       LOAD THE REQUIRED DATABASES  (output of setup_TownsDB.py required)
    #==============================================================================
    TownDB={} #initialize our Town DB
    # load Country DB
    with open('{}CountriesDB.json'.format(PATH),'r') as JSONfile:
        TownDB['COUNTRIES'] = json.loads(JSONfile.read())
    # load list of Country Aliases (language variants, alternative names, nicknames, etc.):
    with open('{}CountryAliasDB.json'.format(PATH),'r') as JSONfile:
        TownDB['COUNTRY_ALIAS'] = json.loads(JSONfile.read())
    # load list of provinces in each country.
    with open('{}ProvinceDB.json'.format(PATH),'r') as JSONfile:
        TownDB['PROVINCES'] = json.loads(JSONfile.read())
    #Create a country-specific DICT of town listings:
    TownDB['TOWNS']={} #make this a dict for simplicity
    dbNames = glob.glob("{}*.{}".format(PATH,'json'))
    for DBs in dbNames:
        DBs = DBs.replace('\\','/') #convert Microsoft GLOB PATH back to UNIX
        fileName = DBs.split('/')[-1].split('.')[0]
        if len(fileName) == 2:  #if true, then it is a country-specific DB of towns
            with open(DBs,'r') as JSONfile:
                TownDB['TOWNS'][fileName] = json.loads(JSONfile.read())
    return TownDB #return the DataSet to the user
    #==============================================================================       
  


def recompileDB(PATH=dataBaseParentPath, Scrape=False, repoID = ArchiveID):
    mdir(PATH)
    if Scrape == True:
        #==============================================================================       
        # ::1::  let's scrape a table containing info on all countries:
        COUNTRIES = getCountries(Verbose=False)
        with open('{}CountriesDB.json'.format(PATH),'w') as file:
            json.dump(COUNTRIES,file)
        #==============================================================================       
        # ::2:: let's scrape tables of towns per country
        uniqueCountries=COUNTRIES['country']
        #CorrectCountries=pd.Series([]) #initiate a pandas series to store the correct country Names (according to our 'correct' country listing)
        for cunts in uniqueCountries:
            if cunts==cunts: #false if cunts is a nan
                fuz=process.extractOne(cunts,COUNTRIES['country'])
                print('\n\nCountry: {} matched as {} with {} probability'.format(cunts,fuz[0],float(fuz[1]/100) ))
                #Great stuff, it works great. Although Northern Ireland is not defined as a Country, and such an entry is 
                #returned as Ireland! Republic of Ireland is returned as Ireland, everything else matches! If this becomes
                #an issue, we could create (likely manually) an alias list for COUNTRIES. 
                #
                #Now let's check if the towns of each country are available:
                correctCountryName=fuz[0]
                iC = COUNTRIES['country'].index(correctCountryName)
                print('i = {}, country 2-char code is {}'.format(iC,COUNTRIES['id2c'][iC]))
                #Great so we can assign a corresponding 2-char country ID to each of these
                #unique countries:
                CountryCode = COUNTRIES['id2c'][iC]
                #scrape town name information!
                Towns=CountryInfo(CountryCode)
                #Compile Alias list:
                Aliases=AliasList(Towns['AliasTownName'])
                #merge results with Town dict:
                Towns['Aliases'] = Aliases['Aliases']
                Towns['AliasIndex'] = Aliases['Index']
                del Towns['AliasTownName']
                print('writing JSON file {}...'.format(CountryCode+'.json'))
                with open('{}{}.json'.format(PATH,CountryCode),'w') as jsonFile:
                    json.dump(Towns,jsonFile)
                print('writing complete \n\n')  
        #==============================================================================           
        # ::3:: compile a table of provences, and a primary key as 
        #as the country name (standard to our countryListDB), using 2-char country ID to save memory        
        province = []
        countryID = []
        i=0
        dbNames = glob.glob("{}*.{}".format(PATH,'json')) #search for json files in the countryDB DIR
        for js in dbNames:
            fileName = js.split('/')[-1].split('.')[0]    #fileName of JSON file (parsed)
            if len(fileName) == 2:  #if fileName is 2-char in length, then it is a Country DB, proceed::
                i+=1
                with open(js,'r') as jsonHandle:
                    CountryDB = json.loads(jsonHandle.read())
                Provinces = pd.Series(CountryDB['Address2'])
                UniProv = Provinces.unique()
                for up in UniProv:
                    province.append(up)
                    countryID.append(fileName)
                print('i ={} {}: {} unique provinces found'.format(i,fileName,len(UniProv)))
        #Save the provincesDB:
        with open('{}ProvinceDB.json'.format(PATH),'w') as file:
            json.dump({'province':province, 'countryID':countryID},file) 
        #==============================================================================       
        # ::4:: Scrape alternative names from WikiPedia!
        coding='lxml'  #coding='html5lib'
        countryAliasList = []
        countryID =[]
        #
        urlList = ["https://en.wikipedia.org/wiki/List_of_country_names_in_various_languages_(A%E2%80%93C)",
            "https://en.wikipedia.org/wiki/List_of_country_names_in_various_languages_(D%E2%80%93I)",
            "https://en.wikipedia.org/wiki/List_of_country_names_in_various_languages_(J%E2%80%93P)",
            "https://en.wikipedia.org/wiki/List_of_country_names_in_various_languages_(Q%E2%80%93Z)"]
        for url in urlList:
            website_url = requests.get(url).text
            soup = BeautifulSoup(website_url,coding)
            My_table = soup.find('table',{'class':'wikitable'}) 
            tr_BS=My_table.findAll('tr')
            #
            try:
                tr_BS=My_table.findAll('tr')
                isTable = True
            except:
                isTable = False
            #
            #parse the data; exit loop if     
            if isTable:
                #Print out the table contents (row, column, 'information')
                for i in range(len(tr_BS)):
                    try:
                        nation=str(tr_BS[i].findAll('a')[0]['title'])
                        iC = COUNTRIES['country'].index(nation)
                        coCode = COUNTRIES['id2c'][iC]
                        goodRow=True
                    except:
                        goodRow=False
                    if goodRow:
                        if len(tr_BS[i].findAll('td')[1].findAll('b')): #if aliases present
                            for ii in range(len(tr_BS[i].findAll('td')[1].findAll('b'))):
                                try:
                                    countryAliasList.append(str(tr_BS[i].findAll('td')[1].findAll('b')[ii])[3:-4])
                                    #countryID.append(tr_BS[i].findAll('a')[0]['title']) #name of country:         
                                    countryID.append(coCode)
                                except:
                                    print('Error appending, skipping!')
        #Save the CountryAliasDB:
        with open('{}CountryAliasDB_scraped.json'.format(PATH),'w') as file:
            json.dump({'countryAlias':countryAliasList, 'countryID':countryID},file)       
    else:
        print('Downloading Archive dataset from google Drive...')
        #with urllib.request.urlopen(repoURL) as response:
        download_file_from_google_drive(repoID,'{}CountryArchive.zip'.format(PATH))   
        print('Unpacking Archive...')
        with zipfile.ZipFile('{}CountryArchive.zip'.format(PATH),'r') as zip_ref:
            zip_ref.extractall(PATH)
        print('Unpacking Complete')
    #==============================================================================       
    # ::5:: Compute a manual alias listing! (try not to put provinces or states in here!) 
    #I can't find an easy way to automate this from a willing source, so I will just roll up the sleeves and get'er done!
    with open('{}CountryAliasDB_scraped.json'.format(PATH),'r') as JSONfile:
        CountryAliases = json.loads(JSONfile.read())
    aliasDB = {}
    aliasDB['GB'] = ['Great Britain', 'Britain','British Isles','UK','U. Kingdom','N. Ireland','Northern Ireland','Scotland','Wales','England', 'Brittania']
    aliasDB['US'] = ['USA', 'US', 'Uncle Sam']
    aliasDB['IE'] = ['Republic of Ireland', 'EIRE', 'Eireann', 'Emearald Isle']
    aliasDB['AU'] = ['Oz','stralia', 'Tasssie','Outback']
    '''aliasDB[''] = ['Republic of Abkhazia','Aphsny Axwynthkharra','Respublika Abkhaziya','Autonomous Republic of Abkhazia'  ]   #Republic of Abkhazia
    aliasDB[''] = ['Islamic Republic of Afghanistan','Da Afġānistān Islāmī Jumhoryat','omhūrīyyeh Eslāmīyyeh Afġānestān']  #afghanistan
    aliasDB[''] = ['Republic of Albania', 'Republika e Shqipërisë','Arnavutluk','Arbanon']
    aliasDB[''] = ['Peoples Democratic Republic of Algeria','al-Jazā’ir']
    aliasDB[''] = []
    aliasDB[''] = []
    aliasDB[''] = []
    countryAliasList=[]
    countryID=[]'''
    for coCode in aliasDB.keys():
        for a in aliasDB[coCode]:
            CountryAliases['countryAlias'].append(a)
            CountryAliases['countryID'].append(coCode)
    #Save the CountryAliasDB:
    with open('{}CountryAliasDB.json'.format(PATH),'w') as file:
        json.dump({'countryAlias':CountryAliases['countryAlias'], 'countryID':CountryAliases['countryID']},file)       


def capFix(strs):
    parts = strs.split(' ')
    if type(parts) == list:
        for i in range(len(parts)):
            try:
                if parts[i] not in ['and', 'of' ]:
                    parts[i] = parts[i][0].upper() + parts[i][1:]
            except:
                pass
        #' '.join(strs.split(' '))
    else:
        try:
            if parts not in ['and', 'of' ]:
                parts = parts[0].upper() + parts[1:]
        except:
            pass
    return ' '.join(parts)
            


def queryCityName(CityEntry,townsDB, countryID=None, threshold = 0.95,lowestAllowedThreshold = 0.65, Ver=False):
    if (CityEntry != CityEntry) or (CityEntry == None) or (CityEntry == 'nan') or (CityEntry == np.nan):
        return [None, 0.0, None, None] #if the Entry is a Nan or empty, return negative
    try :
        CityEntry = capFix(CityEntry)
    except:
        pass
    Possibilities=[]  #empty list to store results that don't meet the threshold liklihood for returning a match
    #------------------------------------
    if countryID == None:    #No CountryID (Primary Key) is given, let's scan all countries!
        for coCode in townsDB['TOWNS']:
            result = pickBestQuery(CityEntry,townsDB['TOWNS'][coCode]['TownName'],Verbose=Ver)
            if result[0] != None: #if result is successful:
                result.append(townsDB['TOWNS'][coCode]['TownName'].index(result[0]))
            else:
                result.append(None)
            result.append(coCode)
            Possibilities.append(result) #append the results to a list in case we need them later
            if result[1] >= threshold:
                return result #If a suitable liklihood is found, return this!
        #If we get to here, then the town      #Let's check town aliases!
        for coCode in townsDB['TOWNS']:
            result = pickBestQuery(CityEntry,townsDB['TOWNS'][coCode]['Aliases'],Verbose=Ver)
            if result[0] != None: #if result is successful:
                idxAlias = townsDB['TOWNS'][coCode]['Aliases'].index(result[0])
                townIdx = townsDB['TOWNS'][coCode]['AliasIndex'][idxAlias]
                result.append(townIdx)
                result.append(coCode)
                result.append(result[0])
                result[0] = townsDB['TOWNS'][coCode]['TownName'][townIdx] #update it with offically accepted TownName
            else:
                result.append(None)
                result.append(None)
            Possibilities.append(result)
            if result[1] >= threshold:
                return result #again, if likelihood is sufficient, return the match!
        #If we get to here, then the townName and townAliases don't match, time to check all results and see if any were satisfactory:
        probScores = [row[1] for row in Possibilities]
        #maxIdx = max(range(len(Possibilities)), key=Possibilities.__getitem__ )
        maxIdx = max(range(len(probScores)), key=probScores.__getitem__ )
        if Possibilities[maxIdx][1] > lowestAllowedThreshold:
            return Possibilities[maxIdx]
        else:
            #No matches satisfy thresholds, return none!
            return [None, 0.0, None, None]
    #------------------------------------
    ## Here we have a countryID specified
    elif type(countryID) == str and len(countryID) == 2: #check if countryID  is correct
        result = pickBestQuery(CityEntry,townsDB['TOWNS'][countryID]['TownName'],Verbose=Ver)
        if result[0] != None: #if result is successful:
            result.append(townsDB['TOWNS'][countryID]['TownName'].index(result[0]))
        result.append(countryID)
        Possibilities.append(result) #append the results to a list in case we need them later
        if result[1] >= threshold:
            return result #If a suitable liklihood is found, return this!    
        else:
            result = pickBestQuery(CityEntry,townsDB['TOWNS'][countryID]['Aliases'],Verbose=Ver)
            if result[0] != None: #if result is successful:
                idxAlias = townsDB['TOWNS'][countryID]['Aliases'].index(result[0])
                townIdx = townsDB['TOWNS'][countryID]['AliasIndex'][idxAlias]
                result.append(townIdx)
                result.append(countryID)
                result.append(result[0])
                result[0] = townsDB['TOWNS'][countryID]['TownName'][townIdx] #update it with offically accepted TownName
            else:
                result.append(None)
                result.append(None)
            Possibilities.append(result)
            if result[1] >= threshold:
                return result
            else:
                probScores = [row[1] for row in Possibilities]
                #maxIdx = max(range(len(Possibilities)), key=Possibilities.__getitem__ )
                maxIdx = max(range(len(probScores)), key=probScores.__getitem__ )
                if Possibilities[maxIdx][1] > lowestAllowedThreshold:
                    return Possibilities[maxIdx]
                else:
                    #No matches satisfy thresholds, return none!
                    return [None, 0.0, None, None]
    else:
        print('error: input argument countryID is unknown, make sure it is of type "str" and length 2')
        #return -1;
        return [None, 0.0, None, None] #entry is wrong
    #
    return result




def queryCountryName(CountryEntry, townsDB, threshold = 0.95, lowestAllowedThreshold = 0.65, Ver=False):
    if (CountryEntry != CountryEntry) or (CountryEntry == None) or (CountryEntry == 'nan') or (CountryEntry == np.nan):
        return [None,0.0] #if the Entry is a Nan or empty, return negative
    try :
        CountryEntry = capFix(CountryEntry)
    except:
        pass
    Possibilities=[]
    #Firstly, try the standardised GeoName list of countries:
    result = pickBestQuery(CountryEntry,townsDB['COUNTRIES']['country'],Verbose=Ver)
    if result[0] != None: #if result is successful:
        iC = townsDB['COUNTRIES']['country'].index(result[0])
        result[0] = townsDB['COUNTRIES']['id2c'][iC] #change country to it's 2-char identifier
    if result[1] >= threshold:
        return result
    else:
        Possibilities.append(result) #save it incase we need it later
        #Second, let's try our list of Aliases (multilingual, nicknames, Abbreviations, variations etc.)
        result = pickBestQuery(CountryEntry,townsDB['COUNTRY_ALIAS']['countryAlias'],Verbose=Ver)
        if result[0] != None: #if result is successful:
            iCa = townsDB['COUNTRY_ALIAS']['countryAlias'].index(result[0])
            result[0] = townsDB['COUNTRY_ALIAS']['countryID'][iCa] #change country to it's 2-char identifier
        if result[1] >= threshold:
            return result
        else:
            Possibilities.append(result) #save it incase we need it later
            #Third, let's try our list of Capitals
            result = pickBestQuery(CountryEntry,townsDB['COUNTRIES']['capital'],Verbose=Ver)
            if result[0] != None: #if result is successful:
                iCc = townsDB['COUNTRIES']['capital'].index(result[0])
                result[0] = townsDB['COUNTRIES']['id2c'][iCc] #change country to it's 2-char identifier
            if result[1] >= threshold:
                return result
            else:
                Possibilities.append(result) #save it incase we need it later
                #Fourth, let's try our list of Provinces
                result = pickBestQuery(CountryEntry,townsDB['PROVINCES']['province'],Verbose=Ver)
                if result[0] != None: #if result is successful:
                    iCp = townsDB['PROVINCES']['province'].index(result[0])
                    result[0] = townsDB['PROVINCES']['countryID'][iCp] #change country to it's 2-char identifier
                if result[1] >=threshold:
                    return result
                else:
                    Possibilities.append(result)
                    #Here we are out of possibilities, we can select the best of the 
                    #results and see if it is above a second more-relaxed threshold.
                    #If not, we can return Zero.
                    probScores = [row[1] for row in Possibilities]
                    #maxIdx = max(range(len(Possibilities)), key=Possibilities.__getitem__ )
                    maxIdx = max(range(len(probScores)), key=probScores.__getitem__ )
                    if Possibilities[maxIdx][1] > lowestAllowedThreshold:
                        return Possibilities[maxIdx]
                    else:
                        #return 0
                        #return Possibilities[maxIdx]
                        return [None,0.0]


#==============================================================================
#  SubFunctions      
#==============================================================================
def pickBestQuery(Query, StandardList, gammaParameter = 1.0, Verbose=False):
    #gammaParameter=1e1 #strength of population weighing, must be =<0, and recommend not going above 10 to minimise power-law error propogation
    try:
        candidates=process.extractBests(Query,StandardList,limit=10)    
    except:
        return [None, 0.0]
    score=[]
    for c in candidates:
        if Verbose:
            print('{} with {} probability'.format(c[0],c[1]))
            print('{} with {} probability (weighed) & diflib'.format(c[0],c[1]*StrMatcher(Query,c[0])))
        score.append(c[1]*(StrMatcher(Query,c[0])/100))
    #Find the maximum weighed score and return best matching country:
    maxIdx = max(range(len(score)), key=score.__getitem__)
    return [candidates[maxIdx][0], score[maxIdx]]
                

    
def StrMatcher(str1, str2):
    if type(str1) is list and type(str2) is list:
        isList = True
        if len(str1) != len(str2):
            return -1 #input lists are not the same length
    elif type(str1) is str and type(str2) is str:
        #this is also fine
        isList= False
    else:
        # argV have different types, so let's grow the string into a list of same length:
        if type(str2) is str: #make str1 the small one, if it isn't, then perform swap:
            temp=str2
            str2=str1
            str1=temp
        temp=[]
        for i in range(len(str2)):
            temp.append(str1)
        str1=temp #doing a soft copy here, as hard copy should not be needed
        isList = True
        #perform operations:
    if isList:
        OUTPUT=[]
        for ii in range(len(str1)):
            print('Length of str1 is {}'.format(len(str1)))
            print('Length of str2 is {}'.format(len(str2)))
            OUTPUT.append( difflib.SequenceMatcher(None, str1[ii],str2[ii]).ratio() )
    else:
        OUTPUT = difflib.SequenceMatcher(None, str1,str2).ratio()
    return OUTPUT    
    
    
    
    
    
    
    
def CountryInfo(CountryCode, Verbose=False):
    fullTable = 50 #the expected number of rows for a full table (50 rows per page)
    numTabCols = 6
    #Allocate memory for the data lists:
    TownIndex = []
    TownAliases = []
    TownNames = []
    TownPopulation = []
    ProvinceAddress = []
    CountyAddress = []
    TownClass = []
    TownLat = []
    TownLon = []
    CountryName = []
    SubAddressName = []
    #
    pageNum = 0
    while True:
        if Verbose: print('page {}, i={}'.format(pageNum//fullTable+1,pageNum))
        url='http://www.geonames.org/search.html?q=&country={}&startRow={}'.format(CountryCode,pageNum)
        website_url = requests.get(url).text
        soup = BeautifulSoup(website_url,coding)
        if Verbose: print('Original encoding found: {}'.format(soup.original_encoding))
        #print(soup.prettify())
        My_table = soup.find('table',{'class':'restable'})    
        try:
            tr_BS=My_table.findAll('tr')
            isTable = True
        except:
            if Verbose: print('Table not found on page {}, skipping...'.format(pageNum//fullTable+1))
            isTable = False
        #parse the data; exit loop if     
        iCaptured = 0 # number of rows captured
        if isTable:
            #Print out the table contents (row, column, 'information')
            for i in range(len(tr_BS)):
                if (len(tr_BS[i]) == numTabCols): #ensure that there are the right number of Cols
                    #we will test that the zeroth column is a table row index (int)
                    try:
                        rowIdx=int(tr_BS[i].findAll('small')[0].contents[0])/1
                        if Verbose: print('table row {}:\n'.format(rowIdx))
                        goodRow = True
                    except:
                        if Verbose: print('header table entry, skipping...')
                        goodRow = False
                    finally:
                        if goodRow:
                            iCaptured+=1
                            #Index:
                            try:
                                TownIndex.append(int(tr_BS[i].findAll('small')[0].contents[0]))
                            except:
                                if Verbose: print('Cannot parse Town index,setting to nan')    
                                TownIndex.append(None)
                            #City Name Alias:
                            try:
                                TownAliases.append(str(tr_BS[i].findAll('small')[1].contents[0]))
                            except:
                                if Verbose: print('Cannot parse Town index,setting to nan')    
                                TownAliases.append(None)
                            #City/town Name Official:
                            try:
                                TownNames.append(str(tr_BS[i].findAll('a')[1].getText()))
                            except:
                                if Verbose: print('Cannot parse Town index,setting to nan')    
                                TownNames.append(None)
                            #Population
                            try:
                                TownPopulation.append(int(tr_BS[i].findAll('small')[3].contents[0].split(' ')[-1].replace(',','')))
                            except:
                                if Verbose: print('Cannot parse Town Population,setting to nan')    
                                TownPopulation.append(None)
                            #Latitude:
                            try:
                                TownLat.append(float(float(tr_BS[i].findAll('span')[1].contents[0])))
                            except:
                                if Verbose: print('Cannot parse Town Latitude,setting to nan')    
                                TownLat.append(None)
                            #Longitude:
                            try:
                                TownLon.append(float(tr_BS[i].findAll('span')[2].contents[0]))
                            except:
                                if Verbose: print('Cannot parse Town Longitude, setting to nan')    
                                TownLon.append(None)
                            #Country:
                            try:
                                CountryName.append(str(tr_BS[i].findAll('a')[3].getText()))
                            except:
                                if Verbose: print('Cannot parse Country Name, setting to nan')    
                                CountryName.append(None)
                            #Address (Province):
                            try:
                                ProvinceAddress.append(str(tr_BS[i].findAll('td')[2].get_text(',').split(',')[2]).strip())   #split index was -3, and was giving wrong results for lots of countries
                            except:
                                if Verbose: print('Cannot parse Sub address, setting to nan')    
                                ProvinceAddress.append(None)
                            #Address (County):
                            try:
                                CountyAddress.append(str(tr_BS[i].findAll('td')[2].get_text(',').split(',')[3]).strip().split('>')[-1])
                            except:
                                if Verbose: print('Cannot parse Sub address, setting to nan')    
                                CountyAddress.append(None)
                              
                            #Address 2:
                            try:
                                 SubAddressName.append(str(tr_BS[i].findAll('small')[2].contents[0]))
                            except:
                                if Verbose: print('Cannot parse Sub address, setting to nan')    
                                SubAddressName.append(None)
                            #Class
                            try:
                                TownClass.append(str(tr_BS[i].findAll('td')[3].contents[0]))
                            except:
                                if Verbose: print('Cannot parse Town index,setting to nan')    
                                TownClass.append(None)
                            #End of parsing
        if iCaptured == fullTable: #Check if there is a full page of results, otherwise consider it final page
            pageNum+=fullTable
        else:   #last page, break out of parsing loop!
            break
    return {'TownIndex':TownIndex ,'TownName':TownNames, 'Country':CountryName, 'AliasTownName':TownAliases,'Latitude':TownLat,'Longitude':TownLon,'Province':ProvinceAddress,'County':CountyAddress,'Address2':SubAddressName,'TownClass':TownClass,'Population':TownPopulation}






def AliasList(Aliases):
    '''AliasList takes in a N-row, single-column list of comma-separated aliases
    and concatenates them into one long single column OUTPUT list. Minor filtering
    on the Alias entries are performed during this routine.
    Brian Scanlon, April, 2019'''
    Idx=[]
    OUTPUT=[]
    for i in range(len(Aliases)):
        try:
            #bufr=Aliases[i].split(',')
            bufr=DelimAliases.split(Aliases[i].strip('.'))
            isOK=True
        except:
            isOK=False
        finally:
            if isOK:
                for ii in range(len(bufr)):
                    Idx.append(i)
                    OUTPUT.append(bufr[ii])
    return {'Index':Idx, 'Aliases':OUTPUT}
 
    





#Find a list of countries!
def getCountries(countryURL='https://www.geonames.org/countries/', Verbose=False):
    #allocate memory to output var:
    idA2 = []
    idA3 = []
    idNum = []
    idFips = []
    country = []
    capital = []
    areaKM2 = []
    population = []
    continentID = []
    #
    numTabCols=9 #manually determined from inspecing the URL / tabled data.
    #
    #Download contents from the URL!
    website_url = requests.get(countryURL).text
    soup = BeautifulSoup(website_url,coding)
    #
    if Verbose: print('Original encoding found: {}'.format(soup.original_encoding))
    #print(soup.prettify())
    My_table = soup.find('table',{'class':'restable'})    
    try:
        tr_BS=My_table.findAll('tr')
        isTable = True
    except:
        print('Table not found!')
        isTable = False
    #parse the data; exit loop if     
    iCaptured = 0 # number of rows captured
    if isTable:
        #Print out the table contents (row, column, 'information')
        for i in range(len(tr_BS)):
            #print('i = {}, columns = {}'.format(i,len(tr_BS[i])))
            #print('\r\n\r\n i = {}  \r\n'.format(i) + tr_BS[i].getText())
            if (len(tr_BS[i]) == numTabCols): #ensure that there are the right number of Cols
                #we will test that the zeroth column is a table row index (int)
                try:
                    CountryID=tr_BS[i].findAll('td')[0].getText()
                    if Verbose: print('table row {}, Country ID = {}:\n'.format(iCaptured,CountryID))
                    goodRow = True
                except:
                    if Verbose: print('header table entry, skipping...')
                    goodRow = False
                finally:
                    if goodRow:
                        iCaptured+=1
                        #ISO-3166 alpha2 country (2-char) Identifier:
                        try:
                            #idA2.append(tr_BS[i].findAll('td')[0].getText())
                            idA2.append(CountryID)
                        except:
                            if Verbose: print('Cannot parse 2-char Country ID, setting to nan')    
                            idA2.append(None)
                        #ISO-3166 alpha3 country (2-char) Identifier:
                        try:
                            idA3.append(tr_BS[i].findAll('td')[1].getText())
                        except:
                            if Verbose: print('Cannot parse 3-char Country ID, setting to nan')    
                            idA3.append(None)
                        #ISO-3166 numeric country (2-char) Identifier:
                        try:
                            idNum.append(tr_BS[i].findAll('td')[2].getText())
                        except:
                            if Verbose: print('Cannot parse 3-char Country ID, setting to nan')    
                            idNum.append(None)
                        #fips country (2-char) Identifier:
                        try:
                            idFips.append(tr_BS[i].findAll('td')[3].getText())
                        except:
                            if Verbose: print('Cannot parse 2-char fips Country ID,setting to nan')    
                            idFips.append(None)
                        #Country Name:
                        try:
                            country.append(tr_BS[i].findAll('td')[4].getText())
                        except:
                            if Verbose: print('Cannot parse Country Name, setting to nan')    
                            country.append(None)
                        #City/town capital of country:
                        try:
                            capital.append(tr_BS[i].findAll('td')[5].getText())
                        except:
                            if Verbose: print('Cannot parse Name of Capital, setting to nan')    
                            capital.append(None)
                        #Area of Country in km^2:
                        try:
                            areaKM2.append(float(tr_BS[i].findAll('td')[6].getText()))
                        except:
                            if Verbose: print('Cannot parse country area, setting to nan')    
                            areaKM2.append(None)
                        #Population:
                        try:
                            population.append(int(tr_BS[i].findAll('td')[7].getText().replace(',','')))
                        except:
                            if Verbose: print('Cannot parse country population, setting to nan')    
                            population.append(None)
                        #Continent ID:
                        try:
                            continentID.append(tr_BS[i].findAll('td')[8].getText())
                        except:
                            if Verbose: print('Cannot parse 2-char continent ID, setting to nan')    
                            continentID.append(None)
                        #End of parsing
        return {'id2c':idA2, 'id3c':idA3, 'id3n':idNum,'idFips':idFips,'country':country, \
        'capital':capital,'area':areaKM2,'population':population,'id_continent':continentID}
    else:
        return None
    
    
    
    
def mdir(PATH):  #Function to make a directory if it doesn't already exist
    if os.path.isdir(PATH) == False:
        dirrs=PATH.split('/')
        pathDump=''
        for dirr in dirrs:
            if dirr == '.':    
                pathDump += dirr
            else:
                pathDump += '{}{}'.format('/',dirr)
            #print('{} : {}'.format(os.path.isdir(pathDump),pathDump))
            if os.path.isdir(pathDump) != True:
                os.mkdir(pathDump)
                
                from requests import get  # to make GET request

'''
from requests import get
def downloader(url, file_name):
    # open in binary mode
    with open(file_name, "wb") as file:
        # get request
        response = get(url)
        # write to file
        file.write(response.content)
'''        
        
def download_file_from_google_drive(id, destination):
    URL = "https://docs.google.com/uc?export=download"

    session = requests.Session()

    response = session.get(URL, params = { 'id' : id }, stream = True)
    token = get_confirm_token(response)

    if token:
        params = { 'id' : id, 'confirm' : token }
        response = session.get(URL, params = params, stream = True)

    save_response_content(response, destination)    

def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            return value

    return None

def save_response_content(response, destination):
    CHUNK_SIZE = 32768

    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)

