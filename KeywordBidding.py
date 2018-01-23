#IMPORT NECESSARY PACKAGES

import pandas as pd
import numpy as np
import re

#IMPORT DATA

Inventory_Current_Onsite=pd.read_csv("Inventory_Current_Onsite.csv")
Inventory_Historical=pd.read_csv("Inventory_Historical.csv")
KW_Attributes=pd.read_csv("KW_Attributes.csv")
KW_Performance_L120D=pd.read_csv("KW_Performance_L120D.csv")
Make_Model_ASR=pd.read_csv("Make_Model_ASR.csv")

#INSPECT DATA

print("\n INSPECT DATA: \n")
print("\n Inventory_Current_Onsite: \n")
print(Inventory_Current_Onsite.info())
print("\n Inventory_Historical: \n")
print(Inventory_Historical.info())
print("\n KW_Attributes: \n")
print(KW_Attributes.info())
print("\n KW_Performance_L120D: \n")
print(KW_Performance_L120D.info())
print("\n Make_Model_ASR: \n")
print(Make_Model_ASR.info())
print("\n Make_Model_ASR Tail:\n")
print(Make_Model_ASR.tail(92))

''' 
CLEANING NOTES:

* No missing values observed
* One empty column in Inventory_Historical
* Few empty trailing rows in Make_Model_ASR
* Following columns need to be converted to integer/float type after removing charecters like "$" and/or ",":
   Est First Pos. Bid (KW_Attributes)
   Est Top of Page Bid (KW_Attributes)
   Cost (KW_Performance_L120D)
   Impressions (KW_Performance_L120D)
   Clicks (KW_Performance_L120D)
   ASR (Make_Model_ASR)

'''

#CLEAN DATA

Make_Model_ASR=Make_Model_ASR.dropna(how='all')
Inventory_Historical=Inventory_Historical.drop(Inventory_Historical.columns[4],axis=1)
KW_Attributes['Est First Pos. Bid'] = KW_Attributes['Est First Pos. Bid'].apply(lambda x: float(re.sub('\\$|,','',x)))
KW_Attributes['Est Top of Page Bid'] = KW_Attributes['Est Top of Page Bid'].apply(lambda x: float(re.sub('\\$|,','',x)))
Make_Model_ASR['ASR'] = Make_Model_ASR['ASR'].apply(lambda x: float(re.sub('\\$|,', '',x)))
KW_Performance_L120D['Cost'] = KW_Performance_L120D['Cost'].apply(lambda x: float(re.sub('\\$|,','',x)))
KW_Performance_L120D['Impressions'] = KW_Performance_L120D['Impressions'].apply(lambda x: int(re.sub(',','',x)))
KW_Performance_L120D['Clicks'] = KW_Performance_L120D['Clicks'].apply(lambda x: int(re.sub(',', '',x)))


#EXTRACT ATTRIBUTES (MKT, MAKE, MODEL and YEAR FROM AD GROUP)

KW_Attributes['MKT']=KW_Attributes['Ad group'].apply(lambda x: re.split('-|_',x)[2])
KW_Attributes['Make']=KW_Attributes['Ad group'].apply(lambda x: re.split('-|_',x)[4])
KW_Attributes['Model']=KW_Attributes['Ad group'].apply(lambda x: re.split('-|_',x)[6])
KW_Attributes['Year']=KW_Attributes['Ad group'].apply(lambda x: int("20"+re.split('-|_',x)[8]))

#DATA INTEGRITY CHECKS

print("\n DATA INTEGRITY CHECKS: \n")
print("\n Check for Duplicates: \n")
print("\n KW_Attributes has " + str(KW_Attributes[['KW ID']].duplicated().sum())+ " duplicates in KW ID \n")
print("\n KW_Performance_L120D has " + str(KW_Performance_L120D[['KW ID']].duplicated().sum())+ " duplicates in KW ID \n")
print("\n Inventory_Historical has " + str(Inventory_Historical[['Make','Model','Year']].duplicated().sum())+ " duplicates in [Make,Model,Year] \n")
print("\n Inventory_Current_Onsitel has " + str(Inventory_Current_Onsite[['Make','Model','Year']].duplicated().sum())+ " duplicates in [Make,Model,Year] \n")
print("\n Make_Model_ASR has " + str(Make_Model_ASR[['Make','Model']].duplicated().sum())+ " duplicates in [Make,Model] \n")
print("\n Referential Integrity Check: \n")
merged = pd.merge(KW_Attributes,KW_Performance_L120D, on='KW ID',how='outer', indicator=True)
print("\n There are " + str(np.sum(merged['_merge']!='both'))+ " rows between KW_Attributes and KW_Performance_L120D, that do not match when joined on KW ID \n") 
merged = pd.merge(KW_Attributes,Inventory_Historical,on=['Make','Model','Year'],how='outer', indicator=True)
print("\n There are " + str(np.sum(merged['_merge']!='both'))+ " rows between KW_Attributes and Inventory_Historical, that do not match when joined on [Make,Model,Year] \n")
merged = pd.merge(KW_Attributes,Inventory_Current_Onsite,on=['Make','Model','Year'],how='outer', indicator=True)
print("\n There are " + str(np.sum(merged['_merge']!='both'))+ " rows between KW_Attributes and Inventory_Current_Onsite, that do not match when joined on [Make,Model,Year] \n")
merged = pd.merge(KW_Attributes,Make_Model_ASR,on=['Make','Model'],how='outer', indicator=True)
print("\n There are " + str(np.sum(merged['_merge']!='both'))+ " rows between KW_Attributes and Make_Model_ASR, that do not match when joined on [Make,Model] \n")



'''
DATA INTEGRITY CHECKS - SUMMARY:
No duplicates found in unique keys (KW ID) and composite unique keys ([Make,Model,Year] & [Make,Model])
No referential integrity violations observed

'''

#PREPARE DATA

KW_Attributes=KW_Attributes.merge(KW_Performance_L120D,on='KW ID',how='inner')
KW_Attributes['CVR_Key Word']=KW_Attributes['Conversions']/KW_Attributes['Clicks']
KW_Attributes['Conversions_Ad_group']=KW_Attributes.groupby('Ad group')['Conversions'].transform(np.sum)
KW_Attributes['CVR_Ad_group']=KW_Attributes['Conversions_Ad_group']/(KW_Attributes.groupby('Ad group')['Clicks'].transform(np.sum))
KW_Attributes['Conversions_Make_Model_Year']=KW_Attributes.groupby(['Make','Model','Year'])['Conversions'].transform(np.sum)
KW_Attributes['CVR_Make_Model_Year']=KW_Attributes['Conversions_Make_Model_Year']/(KW_Attributes.groupby(['Make','Model','Year'])['Clicks'].transform(np.sum))
KW_Attributes['Conversions_Make_Model']=KW_Attributes.groupby(['Make','Model'])['Conversions'].transform(np.sum)
KW_Attributes['CVR_Make_Model']=KW_Attributes['Conversions_Make_Model']/(KW_Attributes.groupby(['Make','Model'])['Clicks'].transform(np.sum))
KW_Attributes['Conversions_MKT']=KW_Attributes.groupby('MKT')['Conversions'].transform(np.sum)
KW_Attributes['CVR_MKT']=KW_Attributes['Conversions_MKT']/(KW_Attributes.groupby('MKT')['Clicks'].transform(np.sum))
KW_Attributes=KW_Attributes.merge(Inventory_Historical,on=['Make','Model','Year'],how='inner') 
KW_Attributes=KW_Attributes.merge(Inventory_Current_Onsite,on=['Make','Model','Year'],how='inner')
KW_Attributes=KW_Attributes.merge(Make_Model_ASR[['Make','Model','ASR']],on=['Make','Model'],how='inner')
KW_Attributes=KW_Attributes.set_index('KW ID')

#STEP 1- CALCULATE INITIAL BID

def generate_initial_bid (KW):
    """Uses the logic described in the excercsie 
    and returns an initial bid value for each keyword."""    
    if KW['Conversions'] > 10:
        return (KW['CVR_Key Word']*KW['ASR'])
    elif KW['Conversions_Ad_group']>10:
        return (KW['CVR_Ad_group']*KW['ASR'])
    elif KW['Conversions_Make_Model_Year']>10:
        return (KW['CVR_Make_Model_Year']*KW['ASR'])
    elif KW['Conversions_Make_Model']>10:
        return (KW['CVR_Make_Model']*KW['ASR']) 
    else:
        return (KW['Est First Pos. Bid']) 


KW_Attributes['Bid']=KW_Attributes.apply(generate_initial_bid,axis=1)
              
                                       
#STEP 2- ADJUST CALCULATED BID (VECTORIZED IMPLEMENTATION)   

#a) Adjust bid based on current onsite inventory            
                                    
InvAdj=KW_Attributes['CurrentOnsiteInventory']<KW_Attributes['HistAvgInv']
AdjFactor=((KW_Attributes.loc[InvAdj,'HistAvgInv']-KW_Attributes.loc[InvAdj,'CurrentOnsiteInventory'])/KW_Attributes.loc[InvAdj,'HistAvgInv'])/2
KW_Attributes.loc[InvAdj,'Bid']=(1-AdjFactor)*KW_Attributes.loc[InvAdj,'Bid']

#b) Adjust bid based on Mkt CVR

MktAdj=np.logical_and(KW_Attributes['Conversions_Ad_group']<11 ,np.logical_or(KW_Attributes['Conversions_Make_Model_Year']>10,KW_Attributes['Conversions_Make_Model']>10))
Site_CVR=float(np.sum(KW_Attributes['Conversions']))/float(np.sum(KW_Attributes['Clicks']))
AdjFactor=((KW_Attributes.loc[MktAdj,'CVR_MKT']-Site_CVR)/Site_CVR)/2
KW_Attributes.loc[MktAdj,'Bid']=(1+AdjFactor)*KW_Attributes.loc[MktAdj,'Bid']

#c) Cap bids at reasonable levels, based on their quality score

Q1Adj=KW_Attributes['Quality score']>7
KW_Attributes.loc[Q1Adj,'Bid']=np.minimum(KW_Attributes.loc[Q1Adj,'Bid'],KW_Attributes.loc[Q1Adj,'Est First Pos. Bid'])
Q2Adj=np.logical_and(KW_Attributes['Quality score']>5,KW_Attributes['Quality score']<8)
KW_Attributes.loc[Q2Adj,'Bid']=np.minimum(KW_Attributes.loc[Q2Adj,'Bid'],((0.5*KW_Attributes.loc[Q2Adj,'Est First Pos. Bid'])+(0.5*KW_Attributes.loc[Q2Adj,'Est Top of Page Bid'])))
Q3Adj=KW_Attributes['Quality score']<6
KW_Attributes.loc[Q3Adj,'Bid']=np.minimum(KW_Attributes.loc[Q3Adj,'Bid'],((0.1*KW_Attributes.loc[Q3Adj,'Est First Pos. Bid'])+(0.9*KW_Attributes.loc[Q3Adj,'Est Top of Page Bid'])))

KW_Attributes.loc[KW_Attributes['Bid']>12,'Bid']=12

# Cap bids of broad match KWs

def broad_match_bid_cap(AdGroup):
    """Returns bid cap values for each adgroups, for capping broad match keywords""" 
    Subset=AdGroup[AdGroup['Match type'] == "Exact"]
    return pd.DataFrame({'Bid Cap':np.amin(Subset['Bid'])},index=AdGroup.index)

KW_Attributes['Broad Match Bid Cap']=KW_Attributes.groupby('Ad group').apply(broad_match_bid_cap)

BroadAdj=KW_Attributes['Match type']=="Broad"
KW_Attributes.loc[BroadAdj,'Bid']=np.minimum(KW_Attributes.loc[BroadAdj,'Bid'],KW_Attributes.loc[BroadAdj,'Broad Match Bid Cap'])


#EXPORT BID UPLOAD FILE

#KW_Attributes[['Bid']].to_csv("Bid Upload Python Output.csv")

KW_Attributes.to_csv("Bid Upload Python Output.csv")

