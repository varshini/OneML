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

#### Sales Play Mapping ####
tranches <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\VCTranches.xlsx")


#### Insights into tranches ####
tranches <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\VCTranches.xlsx")
tranches <- tranches %>% select(TPID, `Box #`)
colnames(tranches) <- c("FinalTPID", "Tranche")

model_output1 <- inner_join(tranches, model_output1, by = "FinalTPID")
a <- model_output1 %>% group_by(Tranche) %>% summarise(count = n())
lessthan300 <- filter(model_output1, TotalM365 <= 300)
a <- lessthan300 %>% group_by(Tranche) %>% summarise(count = n())
greaterthan300 <- filter(model_output1, TotalM365 > 300)
a <- greaterthan300 %>% group_by(Tranche) %>% summarise(count = n())

all_prop <- head(model_output1, 11192)
a <- all_prop %>% group_by(Tranche) %>% summarise(count = n())

adds <- filter(all_prop, TotalM365 <= 300)  
a <- adds %>% group_by(Tranche) %>% summarise(count = n())

exp <- filter(all_prop, TotalM365 > 300) 
a <- exp %>% group_by(Tranche) %>% summarise(count = n())


