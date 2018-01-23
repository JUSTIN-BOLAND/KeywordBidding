#IMPORT NECESSARY PACKAGES

library(data.table)
library(dplyr)
library(tidyr)
library(stringr)

#IMPORT DATA

Inventory_Current_Onsite=fread("Inventory_Current_Onsite.csv")
Inventory_Historical=fread("Inventory_Historical.csv")
KW_Attributes=fread("KW_Attributes.csv")
KW_Performance_L120D=fread("KW_Performance_L120D.csv")
Make_Model_ASR=fread("Make_Model_ASR.csv")

#INSPECT DATA

str(Inventory_Current_Onsite)
str(Inventory_Historical)
str(KW_Attributes)
str(KW_Performance_L120D)
str(Make_Model_ASR)
summary(Inventory_Current_Onsite)
summary(Inventory_Historical)
summary(KW_Attributes)
summary(KW_Performance_L120D)
summary(Make_Model_ASR)

#CLEANING NOTES:

#No missing values observed
#One empty column in Inventory_Historical
#Few empty trailing rows in Make_Model_ASR
#Following columns need to be converted to integer/numeric type after removing charecters like "$" and ",":
#Est First Pos. Bid (KW_Attributes)
#Est Top of Page Bid (KW_Attributes)
#Cost (KW_Performance_L120D)
#Impressions (KW_Performance_L120D)
#Clicks (KW_Performance_L120D)
#ASR (Make_Model_ASR)

#CLEAN DATA

Inventory_Historical$V5=NULL   #Delete empty column
KW_Performance_L120D$Clicks=as.integer((gsub(",", "", KW_Performance_L120D$Clicks))) # Convert Clicks to integer after removing ","
KW_Performance_L120D$Impressions=as.integer((gsub(",", "", KW_Performance_L120D$Impressions))) 
KW_Performance_L120D$Cost=as.numeric((gsub("\\$|,", "", KW_Performance_L120D$Cost))) # Convert to numeric after removing "$" sign and/or ","
Make_Model_ASR=Make_Model_ASR[!apply(Make_Model_ASR == "", 1, all),] # Remove empty rows
Make_Model_ASR$ASR=as.numeric((gsub("\\$|,", "", Make_Model_ASR$ASR))) 
KW_Attributes$`Est First Pos. Bid`= as.numeric((gsub("\\$|,", "", KW_Attributes$`Est First Pos. Bid`))) 
KW_Attributes$`Est Top of Page Bid`= as.numeric((gsub("\\$|,", "", KW_Attributes$`Est Top of Page Bid`))) 


#EXTRACT ATTRIBUTES (MKT, MAKE, MODEL and YEAR FROM AD GROUP)

KW_Attributes$MKT= str_extract(KW_Attributes$`Ad group`,"(?<=SRCH-I-)(.*)(?=-MK_)")# Extract MKT from Ad group
KW_Attributes$Make=str_extract(KW_Attributes$`Ad group`,"(?<=-MK_)(.*)(?=-MO_)")
KW_Attributes$Model=str_extract(KW_Attributes$`Ad group`,"(?<=-MO_)(.*)(?=-YR_)")
KW_Attributes$Year=as.integer(paste0("20",str_extract(KW_Attributes$`Ad group`,"(?<=-YR_)(.*)")))

#DATA INTEGRITY CHECKS

print(sum(duplicated(KW_Attributes$`KW ID`))) #prints 0 if no duplictes found
print(sum(duplicated(KW_Performance_L120D$`KW ID`)))
print(sum(duplicated(Make_Model_ASR$`Make Model`)))
print(sum(duplicated(paste0(Inventory_Current_Onsite$Make,Inventory_Current_Onsite$Model,Inventory_Current_Onsite$Year))))
print(sum(duplicated(paste0(Inventory_Historical$Make,Inventory_Historical$Model,Inventory_Historical$Year))))
print(anti_join(KW_Attributes,KW_Performance_L120D,by="KW ID"))# prints 0 rows if all the rows in KW_Attributes have a match in KW_Performance_L120D by KW ID
print(anti_join(KW_Performance_L120D,KW_Attributes,by="KW ID"))
print(anti_join(KW_Attributes,Inventory_Current_Onsite,by=c("Make","Model","Year")))
print(anti_join(Inventory_Current_Onsite,KW_Attributes,by=c("Make","Model","Year")))
print(anti_join(KW_Attributes,Inventory_Historical,by=c("Make","Model","Year")))
print(anti_join(Inventory_Historical,KW_Attributes,by=c("Make","Model","Year")))
print(anti_join(KW_Attributes,Make_Model_ASR,by=c("Make","Model")))
print(anti_join(Make_Model_ASR,KW_Attributes,by=c("Make","Model")))

#DATA INTEGRITY CHECKS - SUMMARY:
#No duplicates found in unique keys (KW ID) and composite unique keys ("Make","Model","Year" & "Make","Model")
#No referential integrity violations observed


#PREPARE DATA

setkey(KW_Attributes,`KW ID`)
setkey(KW_Performance_L120D,`KW ID`)
KW_Attributes=KW_Attributes[KW_Performance_L120D, nomatch=0] #Join KW_Attributes with KW_Performance_L120D (the data.table way) for fetching performance metrics
rm(KW_Performance_L120D) # remove KW_Performance_L120D as its now redundant
KW_Attributes[,Conversions_Ad_group := sum(Conversions), 
                                                   by = .(`Ad group`)][,Conversions_Make_Model_Year := sum(Conversions),
                                                                       by = .(Make,Model,Year)][,Conversions_Make_Model := sum(Conversions),by = .(Make,Model)]
KW_Attributes[,CVR := Conversions/Clicks][,CVR_Ad_group := sum(Conversions)/sum(Clicks), 
                                                             by = .(`Ad group`)][,CVR_Make_Model_Year := sum(Conversions)/sum(Clicks),
                                                                                 by = .(Make,Model,Year)][,CVR_Make_Model := sum(Conversions)/sum(Clicks),
                                                                                                          by = .(Make,Model)][,CVR_MKT := sum(Conversions)/sum(Clicks),by = .(MKT)]
KW_Attributes=KW_Attributes%>%inner_join(Make_Model_ASR,by=c("Make","Model"))%>%select(-`Make Model`)
rm(Make_Model_ASR)

#STEP 1- CALCULATE INITIAL BID


for (row in 1:nrow(KW_Attributes)) {
  
  if(KW_Attributes[row, "Conversions"] > 10) {
    
    KW_Attributes[row, "Bid"]=KW_Attributes[row, "CVR"]*KW_Attributes[row, "ASR"]
  
    }else if(KW_Attributes[row, "Conversions_Ad_group"] > 10){
    
    KW_Attributes[row, "Bid"]=KW_Attributes[row, "CVR_Ad_group"]*KW_Attributes[row, "ASR"]   
    }else if(KW_Attributes[row, "Conversions_Make_Model_Year"] > 10){
    
    KW_Attributes[row, "Bid"]=KW_Attributes[row, "CVR_Make_Model_Year"]*KW_Attributes[row, "ASR"]   
    }else if(KW_Attributes[row, "Conversions_Make_Model"] > 10){
    
    KW_Attributes[row, "Bid"]=KW_Attributes[row, "CVR_Make_Model"]*KW_Attributes[row, "ASR"]   
    }else{
    
    KW_Attributes[row, "Bid"]=KW_Attributes[row, "Est First Pos. Bid"]
    
    }  
    
}


#STEP 2- ADJUST CALCULATED BID


KW_Attributes=KW_Attributes%>%inner_join(Inventory_Current_Onsite,by=c("Make","Model","Year"))%>%
  inner_join(Inventory_Historical,by=c("Make","Model","Year"))

rm(Inventory_Current_Onsite)
rm(Inventory_Historical)

for (row in 1:nrow(KW_Attributes)) {
  
  
  
  if(KW_Attributes[row, "CurrentOnsiteInventory"] < KW_Attributes[row, "HistAvgInv"]) {
    
    AdjFactor=((KW_Attributes[row, "HistAvgInv"]-KW_Attributes[row, "CurrentOnsiteInventory"])/KW_Attributes[row, "HistAvgInv"])/2
    KW_Attributes[row, "Bid"]=(1-AdjFactor)*KW_Attributes[row, "Bid"]
    
  }
  if(KW_Attributes[row, "Conversions_Ad_group"] < 11){
    
    Site_CVR=sum(KW_Attributes$Conversions)/sum(KW_Attributes$Clicks)
    AdjFactor=((KW_Attributes[row, "CVR_MKT"]-Site_CVR)/Site_CVR)/2
    KW_Attributes[row, "Bid"]=(1+AdjFactor)*KW_Attributes[row, "Bid"]
    rm(AdjFactor)
    rm(Site_CVR)
  }
  if(KW_Attributes[row, "Quality score"] > 7){
    
    KW_Attributes[row, "Bid"]=min(KW_Attributes[row, "Bid"],KW_Attributes[row, "Est First Pos. Bid"])
    
  }else if(KW_Attributes[row, "Quality score"] > 5){
    
    KW_Attributes[row, "Bid"]=min(KW_Attributes[row, "Bid"],(0.5*KW_Attributes[row, "Est First Pos. Bid"]+0.5*KW_Attributes[row, "Est Top of Page Bid"]))
  }else{
    
    KW_Attributes[row, "Bid"]=min(KW_Attributes[row, "Bid"],(0.1*KW_Attributes[row, "Est First Pos. Bid"]+0.9*KW_Attributes[row, "Est Top of Page Bid"]))
    
  } 
  if(KW_Attributes[row, "Bid"]>12)
  {
    KW_Attributes[row, "Bid"]=12
  }

  
}


Temp = KW_Attributes%>% # Bid cap for Broad match type
  filter(`Match type` == 'Exact')%>%
  group_by(`Ad group`)%>%
  summarise(BidCap=min(Bid))

KW_Attributes=KW_Attributes%>%
  inner_join(Temp,by=("Ad group"))

for (row in 1:nrow(KW_Attributes)) {

if(KW_Attributes[row, "Match type"]=="Broad")
{
  KW_Attributes[row, "Bid"]=min(KW_Attributes[row, "Bid"],KW_Attributes[row, "BidCap"])  
}
  
}  

Bid_upload=KW_Attributes[,c("KW ID","Bid")]

rm(KW_Attributes)
rm(Temp)

#EXPORT BID UPLOAD FILE

write.csv(Bid_upload,"Bid Upload R Output.csv")








