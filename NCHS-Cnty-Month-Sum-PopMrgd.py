#!/usr/bin/env python
# coding: utf-8

# ## Import

# In[1]:


import pandas as pd
import os
import numpy as np
from random import sample
from itertools import chain 
import csv
import glob
import os
from functools import reduce


# ## Load ICD, FIPS, and Population

# In[2]:


icd10u = pd.read_csv("icd10_underlying.csv")
icd_underlying=[]
for column in icd10u.columns: 
    icd_underlying = icd10u["icdcode"].tolist() 


# In[3]:


icd10c = pd.read_csv("icd10_contributing.csv")
icd_contributing=[]
for column in icd10c.columns: 
    icd_contributing = icd10c["icdcode"].tolist() 


# In[4]:


statefips = pd.read_csv("statefips.csv")
countyfips = pd.read_csv("countyfips.csv")
statefips['state']=statefips.state_name


# In[5]:


population = pd.read_csv("/Users/serenakim/Dropbox/10-opioid-2019/dataNCHSFilter/seer_county_pop_2000_2018.csv")
poptot = pd.merge(statefips, population, how='left', on=['state_abbr'])
poptot = poptot.rename(columns={'state_name': 'state'})


# ## Automate

# In[6]:


for filename in os.listdir('/Users/serenakim/Dropbox/10-opioid-2019/dataNCHSin/'):
    if filename.endswith(".txt"):
        with open('/Users/serenakim/Dropbox/10-opioid-2019/dataNCHSin/'+filename) as f:
            data = pd.read_csv(f, sep = "\t", header=None)
            headerName=["case"]
            data.columns=headerName
            
            year = filename[4:8] 
            month = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
            month12 = 3142 * month
            countyfips['year'] = year

            countyfips12 = pd.concat([countyfips]*12)
            countyfips12 = countyfips12.sort_values(by='fips', ascending=True)
            countyfips12['month'] = month12

            countyfips12['fips'] = countyfips12.fips.apply(str)
            countyfips12['year'] = countyfips12.year.apply(str)
            countyfips12['fips_bymonth'] = countyfips12['fips'] + countyfips12['year'] + countyfips12['month'] 
            
            countyfips12 = pd.merge(countyfips12, statefips, on=['state'], how='left').fillna('0')
            countyfips12['year_str']=countyfips12.year
            countyfips12['month_str']=countyfips12.month
            countyfips12['yearmonth']=countyfips12['year_str']+countyfips12['month_str']
            countyfips12 = countyfips12.drop(columns=['year_str', 'month_str', 'state_name'])
            
            case_drugod_underlying = data.case.str[145:149]
            case_opioid_contributing = data.case.str[149:443]

            drugod = case_drugod_underlying.str.contains('|'.join(icd_underlying)) #filtering raw data using all icdcode and creating a boolean
            count_drugod = np.count_nonzero(drugod)
            opioidod = case_opioid_contributing.str.contains('|'.join(icd_contributing)) #filtering raw data using all icdcode and creating a boolean
            count_opioidod = np.count_nonzero(opioidod)

            data_s = pd.Series(data["case"])

            s1 = data_s.tolist() 
            s2 = drugod.tolist()
            s3 = opioidod.tolist() # bad names for temporary lists
            d = {'case':s1,'drugod':s2,'opioidod':s3} # merging 
            data_fltd = pd.DataFrame(d) # series to list, list to dict, dict to df

            index = data_fltd[ (data_fltd['drugod'] == False) | (data_fltd['opioidod'] == False)].index #index either is false
            data_fltd.drop(index, inplace=True) 

            '''
            Concatenate
            '''

            state = data_fltd.case.str[32:34].tolist()
            county = data_fltd.case.str[34:37].tolist()
            race_recode05 = data_fltd.case.str[449].tolist()
            hispanic = data_fltd.case.str[487].tolist()
            month = data_fltd.case.str[64:66].tolist()
            age = data_fltd.case.str[78:80].tolist()
            sex = data_fltd.case.str[68].tolist()
            edu89 = data_fltd.case.str[60:62].tolist()
            edu03 = data_fltd.case.str[62].tolist()

            data_fltd['state_abbr'] = state
            data_fltd['county_fips'] = county
            data_fltd['race'] = race_recode05
            data_fltd['hispanic'] = hispanic
            data_fltd['month'] = month
            data_fltd['age'] = age
            data_fltd['sex'] = sex
            data_fltd['edu89'] = edu89
            data_fltd['edu03'] = edu03

            hispanic_s = pd.Series(data_fltd["hispanic"]) 
            screen_hispanic = hispanic_s.str.isnumeric() 
            del_list_hispanic = screen_hispanic.tolist()
            data_fltd['del_hispanic'] = del_list_hispanic
            index_hispanic = data_fltd[ data_fltd['del_hispanic'] == False].index
            data_fltd.drop(index_hispanic, inplace=True) #drop if nonnumeric

            race_s = pd.Series(data_fltd["race"]) 
            screen_race = race_s.str.isnumeric() 
            del_list_race = screen_race.tolist()
            data_fltd['del_race'] = del_list_race
            index_race = data_fltd[ data_fltd['del_race'] == False].index
            data_fltd.drop(index_race, inplace=True) #drop if nonnumeric

            month_s = pd.Series(data_fltd["month"]) 
            screen_month = month_s.str.isnumeric() 
            del_list_month = screen_month.tolist()
            data_fltd['del_month'] = del_list_month
            index_month = data_fltd[ data_fltd['del_month'] == False].index
            data_fltd.drop(index_month, inplace=True) #drop if nonnumeric

            age_s = pd.Series(data_fltd["age"]) 
            screen_age = age_s.str.isnumeric() 
            del_list_age = screen_age.tolist()
            data_fltd['del_age'] = del_list_age
            index_age = data_fltd[ data_fltd['del_age'] == False].index
            data_fltd.drop(index_age, inplace=True) #drop if nonnumeric
            
            edu89_s = pd.Series(data_fltd["edu89"]) 
            screen_edu89 = edu89_s.str.isnumeric() 
            del_list_edu89 = screen_edu89.tolist()
            data_fltd['del_edu89'] = del_list_edu89

            edu03_s = pd.Series(data_fltd["edu03"]) 
            screen_edu03 = edu03_s.str.isnumeric() 
            del_list_edu03 = screen_edu03.tolist()
            data_fltd['del_edu03'] = del_list_edu03

            index_edu = data_fltd[ (data_fltd['del_edu89'] == False) & (data_fltd['del_edu03'] == False)].index

            '''
            Crosswalk
            '''

            state_mrgd = pd.merge(data_fltd, statefips, on=["state_abbr"])

            ctyfips_s = pd.Series(state_mrgd["county_fips"]) 
            str_ctyfips = ctyfips_s.str.isnumeric() #find non-numeric values 
            del_list_ctyfips = str_ctyfips.tolist()
            state_mrgd['del_str_ctyfips'] = del_list_ctyfips #include the boolean column in the master file 
            index = state_mrgd[ state_mrgd['del_str_ctyfips'] == False].index
            state_mrgd.drop(index, inplace=True) #drop if nonnumeric

            state_mrgd['county_fips'] = state_mrgd.county_fips.astype(int)
            state_mrgd['state_fips'] = state_mrgd.state_fips.astype(int)
            state_mrgd["fips"] = state_mrgd["state_fips"] * 1000 + state_mrgd["county_fips"] #fips generated

            state_mrgd['month'] = state_mrgd.month.apply(str)
            state_mrgd['year'] = str(year)
            state_mrgd['fips'] = state_mrgd.fips.apply(str)
            state_mrgd['fips_bymonth']= state_mrgd['fips'] + state_mrgd['year'] + state_mrgd['month']

            count_od_bym = state_mrgd['opioidod'].eq(True).astype(int).groupby(state_mrgd['fips_bymonth']).sum() 
            count = pd.DataFrame({'fips_bymonth':count_od_bym.index, 'opioid_death':count_od_bym.values}) #counts generated

            '''
            Creating Filters
            '''

            state_mrgd['hispanic'] = state_mrgd.hispanic.astype(int)
            hispanic_condition = [
                (state_mrgd['hispanic'] <= 5)
                ]
            hispanic_val = ["hispanic"]
            state_mrgd['hispanic_filter'] = np.select(hispanic_condition, hispanic_val)


            state_mrgd['race'] = state_mrgd.race.astype(int)
            race_condition = [
                (state_mrgd['race'] == 1),
                (state_mrgd['race'] == 2),
                (state_mrgd['race'] == 4)
                ]
            # create a list of the values we want to assign for each condition
            race_val = ["white", "black", "asian"]
            # create a new column and use np.select to assign values to it using our lists as arguments
            state_mrgd['race_ctgr'] = np.select(race_condition, race_val)

            state_mrgd['month'] = state_mrgd.month.astype(int)

            month_condition = [
                (state_mrgd['month'] == 1),
                (state_mrgd['month'] == 2),
                (state_mrgd['month'] == 3),
                (state_mrgd['month'] == 4),
                (state_mrgd['month'] == 5),
                (state_mrgd['month'] == 6),
                (state_mrgd['month'] == 7),
                (state_mrgd['month'] == 8),
                (state_mrgd['month'] == 9),
                (state_mrgd['month'] == 10),
                (state_mrgd['month'] == 11),
                (state_mrgd['month'] == 12)
                ]

            # create a list of the values we want to assign for each condition
            month_val = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"]

            # create a new column and use np.select to assign values to it using our lists as arguments
            state_mrgd['month_ctgr'] = np.select(month_condition, month_val)

            state_mrgd['age'] = state_mrgd.age.astype(int)
            age_condition = [
                (state_mrgd['age'] == 1),
                (state_mrgd['age'] == 2),
                (state_mrgd['age'] == 3),
                (state_mrgd['age'] == 4),
                (state_mrgd['age'] == 5),
                (state_mrgd['age'] == 6),
                (state_mrgd['age'] == 7),
                (state_mrgd['age'] == 8),
                (state_mrgd['age'] == 9),
                (state_mrgd['age'] == 10),
                (state_mrgd['age'] == 11),
                (state_mrgd['age'] == 12)
                ]
            # create a list of the values we want to assign for each condition
            age_val = ["age_under1", "age_1-4", "age_5-14", "age_15-24", "age_25-34", "age_35-44", "age_45-54", "age_55-64", "age_65-74", "age_75-84", "age_85andover", "age_unk"]
            # create a new column and use np.select to assign values to it using our lists as arguments
            state_mrgd['age_ctgr'] = np.select(age_condition, age_val)
            
            state_mrgd['sex'] = state_mrgd.sex
            sex_condition = [
                (state_mrgd['sex'] == "M"),
                (state_mrgd['sex'] == "F"),
                (state_mrgd['sex'] != "M") & (state_mrgd['sex'] != "F") 
                ]
            # create a list of the values we want to assign for each condition
            sex_val = ["male", "female", "na"]
            # create a new column and use np.select to assign values to it using our lists as arguments
            state_mrgd['sex_ctgr'] = np.select(sex_condition, sex_val)
            
            racesex_condition = [
                (state_mrgd['hispanic'] <= 5) & (state_mrgd['sex'] == "M"),
                (state_mrgd['hispanic'] <= 5) & (state_mrgd['sex'] == "F"),
                (state_mrgd['race'] == 1) & (state_mrgd['sex'] == "M"),
                (state_mrgd['race'] == 1) & (state_mrgd['sex'] == "F"),
                (state_mrgd['race'] == 2) & (state_mrgd['sex'] == "M"),
                (state_mrgd['race'] == 2) & (state_mrgd['sex'] == "F"),
                (state_mrgd['race'] == 4) & (state_mrgd['sex'] == "M"),
                (state_mrgd['race'] == 4) & (state_mrgd['sex'] == "F")
                ]
            # create a list of the values we want to assign for each condition
            racesex_val = ["hisp_male", "hisp_female", "white_male", "white_female","black_male", "black_female","asian_male", "asian_female"]
            # create a new column and use np.select to assign values to it using our lists as arguments
            state_mrgd['racesex_ctgr'] = np.select(racesex_condition, racesex_val)
            
            edu_condition = [
                (state_mrgd['edu89'] == "00") | (state_mrgd['edu89'] == "01") | (state_mrgd['edu89'] == "02") | (state_mrgd['edu89'] == "03")| (state_mrgd['edu89'] == "04") | (state_mrgd['edu89'] == "05") | (state_mrgd['edu89'] == "06") | (state_mrgd['edu89'] == "07") | (state_mrgd['edu89'] == "08") | (state_mrgd['edu03'] == "1"),
                (state_mrgd['edu89'] == "09") | (state_mrgd['edu89'] == "10") | (state_mrgd['edu89'] == "11")| (state_mrgd['edu89'] == "12")| (state_mrgd['edu03'] == "2") | (state_mrgd['edu03'] == "3"),
                (state_mrgd['edu89'] == "13") | (state_mrgd['edu89'] == "14")| (state_mrgd['edu89'] == "15")| (state_mrgd['edu89'] == "16") | (state_mrgd['edu03'] == "4") | (state_mrgd['edu03'] == "5") | (state_mrgd['edu03'] == "6"),
                (state_mrgd['edu89'] == "17") | (state_mrgd['edu03'] == "7") | (state_mrgd['edu03'] == "8"),
                (state_mrgd['edu89'] == "99") | (state_mrgd['edu03'] == "9") 
                ]
            # create a list of the values we want to assign for each condition
            edu_val = ["edu_8th", "edu_high", "edu_college", "edu_grad", "edu_unk"]
            # create a new column and use np.select to assign values to it using our lists as arguments
            state_mrgd['edu_ctgr'] = np.select(edu_condition, edu_val)
            
            '''
            Cross Tabulation
            '''

            crosstab_hispanic = pd.crosstab(state_mrgd.fips_bymonth, state_mrgd.hispanic_filter)
            crosstab_hispanic.reset_index(inplace=True)
            crosstab_hispanic = crosstab_hispanic.drop(columns=["0"])

            crosstab_race = pd.crosstab(state_mrgd.fips_bymonth, state_mrgd.race_ctgr)
            crosstab_race.reset_index(inplace=True)
            crosstab_race = crosstab_race.drop(columns=["0"])

            crosstab_age = pd.crosstab(state_mrgd.fips_bymonth, state_mrgd.age_ctgr)
            #crosstab_month = crosstab_month.drop(columns=[0]) #no zeros (already deleted)
            crosstab_age = crosstab_age.reindex(columns=age_val)
            crosstab_age.reset_index(inplace=True)
            
            crosstab_sex = pd.crosstab(state_mrgd.fips_bymonth, state_mrgd.sex_ctgr)
            crosstab_sex.reset_index(inplace=True)
            
            crosstab_racesex = pd.crosstab(state_mrgd.fips_bymonth, state_mrgd.racesex_ctgr)
            crosstab_racesex.reset_index(inplace=True)
            crosstab_racesex = crosstab_racesex.drop(columns=["0"])
            
            crosstab_edu = pd.crosstab(state_mrgd.fips_bymonth, state_mrgd.edu_ctgr)
            crosstab_edu.reset_index(inplace=True)
            cols = crosstab_edu.columns.tolist() # the order of columns mixed for some reason 
            cols = cols[:2] + cols[4:5] + cols[2:4] + cols[5:6] # column order fixed
            crosstab_edu = crosstab_edu[cols]

            '''
            Merging 
            '''

            countyfips12['fips_bymonth']  = countyfips12.fips_bymonth.astype(int)
            count['fips_bymonth'] = count.fips_bymonth.astype(int)
            crosstab_race['fips_bymonth'] = crosstab_race.fips_bymonth.astype(int)
            crosstab_hispanic['fips_bymonth'] = crosstab_hispanic.fips_bymonth.astype(int)
            crosstab_age['fips_bymonth'] = crosstab_age.fips_bymonth.astype(int)
            crosstab_sex['fips_bymonth'] = crosstab_sex.fips_bymonth.astype(int)
            crosstab_racesex['fips_bymonth'] = crosstab_racesex.fips_bymonth.astype(int)
            crosstab_edu['fips_bymonth'] = crosstab_edu.fips_bymonth.astype(int)

            frame = [countyfips12, count, crosstab_race, crosstab_hispanic, crosstab_sex, crosstab_racesex, crosstab_age, crosstab_edu]
            df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['fips_bymonth'],
                                                        how='outer'), frame).fillna('0')

            index_void = df_merged[ df_merged['county'] == "0"].index
            df_merged.drop(index_void, inplace=True)

            df_merged.to_csv('/Users/serenakim/Dropbox/10-opioid-2019/dataNCHSout/' + filename.strip("US.AllCnty.txt") + 'CntySumMonth.csv')

print("Done")


# ## Append

# In[7]:


os.chdir("/Users/serenakim/Dropbox/10-opioid-2019/dataNCHSout/")
extension = 'csv'
all_file = [i for i in glob.glob('*.{}'.format(extension))]
appended = pd.concat([pd.read_csv(f) for f in all_file ])
appended = appended.sort_values(by=['fips', 'year', 'month'])
appended = appended.drop(columns=['Unnamed: 0'])


# In[28]:


population = pd.read_csv("/Users/serenakim/Dropbox/10-opioid-2019/dataNCHSFilter/seer_county_pop_2000_2018.csv")
poptot = pd.merge(statefips, population, how='left', on=['state_abbr'])
poptot = poptot.drop(columns=['state_name'])


# In[32]:


appended.dtypes


# In[33]:


frames = [appended, poptot]
df_pop_merged = reduce(lambda  left,right: pd.merge(left,right,on=['fips', 'state', 'year'],
                                            how='outer'), frames).fillna('0')
index_void = df_pop_merged[ df_pop_merged['county'] == "0"].index
df_pop_merged.drop(index_void, inplace=True)
df_pop_merged = df_pop_merged.drop(columns=['state_fips_x', 'state_abbr_x'])
df_pop_merged = df_pop_merged.rename(columns={'state_fips_y': 'state_fips'})
df_pop_merged = df_pop_merged.rename(columns={'state_abbr_y': 'state_abbr'}) 


# In[ ]:


df_pop_merged = df_pop_merged.opioid_death/df_pop_merged.poptot


# In[25]:


df_pop_merged.to_csv( "/Users/serenakim/Dropbox/10-opioid-2019/dataNCHS/2005_2018CntySumMonth.csv", index=False, encoding='utf-8-sig')


# In[ ]:




