library(readxl)
library(dplyr)
library(tidyverse)
library(data.table)
require(TTR)
require(quantmod)
require(caTools)

smctpids <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\SMCTPIDs.csv")
colnames(smctpids)[1] <- "FinalTPID"
smctpids <- filter(smctpids, FY21.Subsegment %in% c("SM&C Government - Corporate", "Enterprise Growth", "SM&C Commercial - Corporate") ) #27980 Tpids

#### High and Medium propensity Accounts #####
training_data <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\ScoringFeaturesFinalv2.csv")

training_data <- training_data %>% select(FinalTPID, M365F1, M365E3, M365E5, Area, IndustryNum)
training_data$TotalM365 <- training_data$M365F1 + training_data$M365E3 + training_data$M365E5

model_output <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Modeling\\ModelOutputFinalv2.csv")

model_output1 <- inner_join(model_output, training_data)
model_output1 <- model_output1[order(-model_output1$Probability1),]

high_prop <- head(model_output1, 5596)
nrow(filter(high_prop, TotalM365 < 300))  
nrow(filter(high_prop, TotalM365 >= 300)) 

med_prop <- head(model_output1, 11192)
nrow(filter(med_prop, TotalM365 < 300))  
nrow(filter(med_prop, TotalM365 >= 300)) 

low_prop1 <- head(model_output1, 2798) #0.61 - 10%
low_prop2 <- head(model_output1, 4197) #0.54 - 15%
low_prop3 <- head(model_output1, 1399) #0.70 - 5%

### Insights into tranches ####
tranches <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\VCTranches.xlsx")
tranches <- tranches %>% select(TPID, `Box #`)
colnames(tranches) <- c("FinalTPID", "Tranche")

model_output1 <- inner_join(tranches, model_output1, by = "FinalTPID")
model_output1 <- model_output1[order(-model_output1$Probability1),]
a <- model_output1 %>% group_by(Tranche) %>% summarise(count = n())

# lessthan300 <- filter(model_output1, TotalM365 <= 300)
# a <- lessthan300 %>% group_by(Tranche) %>% summarise(count = n())
# greaterthan300 <- filter(model_output1, TotalM365 > 300)
# a <- greaterthan300 %>% group_by(Tranche) %>% summarise(count = n())

all_prop <- head(model_output1, 11192)
a <- all_prop %>% group_by(Tranche) %>% summarise(count = n())

adds <- filter(all_prop, TotalM365 <= 300 & Probability1 >= 0.34)
a <- adds %>% group_by(Tranche) %>% summarise(count = n())

exp <- filter(all_prop, TotalM365 > 300)
a <- exp %>% group_by(Tranche) %>% summarise(count = n())
# 
# 
# ### Reducing the propensity for expansion - GOING TO USE THESE CUTOFFS FOR THE MODEL TO REDUCE THE # OF ACCOUNTS IN EXPANSION ####
# 
# adds <- filter(model_output1, TotalM365 < 300) 
# high_prop_adds <-  head(adds, 4067)
# med_prop_adds <- head(adds, 8134)
# b <- inner_join(med_prop_adds, tranches)
# c <- b %>% group_by(Tranche) %>% summarise(count = n())
# 
# exp <- filter(model_output1, TotalM365 >= 300)
# high_prop_exp <- head(exp, 727)
# med_prop_exp <- head(exp, 1528)
# b <- inner_join(med_prop_exp, tranches)
# c <- b %>% group_by(Tranche) %>% summarise(count = n())
# 
# adds <- filter(adds, Probability1 >= 0.312) #7644
# exp <- filter(exp, Probability1 >= 0.522)#1535
# final_set <- rbind(adds, exp)
# write.csv(final_set, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Modeling\\DROutput\\FinalTPIDSet.csv", row.names = FALSE)
# 
# # FINAL PROBABILITIES FOR MODEL - HIGH/MED ADDS (high - >= 0.477, med - >= 0.312), HIGH/MED EXPANSION (high - >= 0.636, med - >=0.522) #
# # GET FINAL ACCOUNT SET FROM HERE AND USE IT IN CREATEDROUTPUT.R #