# Perpetual Usage 

library(readxl)
library(dplyr)
library(tidyverse)
library(data.table)
require(TTR)
require(quantmod)
require(caTools)
options(stringsAsFactors = FALSE)

training <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\O365Usage\\2019_12_Perpetual.csv")
training <- training %>% group_by(FinalTPID) %>% summarise(O2019_rl28_users = sum(O2019_rl28_users), 
                                                           O2016_rl28_users = sum(O2016_rl28_users),
                                                           O2013_rl28_users = sum(O2013_rl28_users),
                                                           O2010_rl28_users = sum(O2010_rl28_users),
                                                           OPP_rl28_users = sum(OPP_rl28_users))
training$ym <- "201912"
training$O2019Ratio <- training$O2019_rl28_users/training$OPP_rl28_users
training$O2016Ratio <- training$O2016_rl28_users/training$OPP_rl28_users
training$O2013Ratio <- training$O2013_rl28_users/training$OPP_rl28_users
training$O2010Ratio <- training$O2010_rl28_users/training$OPP_rl28_users

training[is.na(training)] <- 0 
training <- rapply(training, f=function(x) ifelse(is.nan(x),0,x), how="replace" )
training <- rapply(training, f=function(x) ifelse(is.infinite(x),0,x), how="replace" )
write.csv(training, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Usage\\Training_Perpetual.csv", row.names =FALSE)

scoring <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\O365Usage\\2020_06_Perpetual.csv")
scoring <- scoring %>% group_by(FinalTPID) %>% summarise(O2019_rl28_users = sum(O2019_rl28_users), 
                                                           O2016_rl28_users = sum(O2016_rl28_users),
                                                           O2013_rl28_users = sum(O2013_rl28_users),
                                                           O2010_rl28_users = sum(O2010_rl28_users),
                                                           OPP_rl28_users = sum(OPP_rl28_users))
scoring$ym <- "202005"
scoring$O2019Ratio <- scoring$O2019_rl28_users/scoring$OPP_rl28_users
scoring$O2016Ratio <- scoring$O2016_rl28_users/scoring$OPP_rl28_users
scoring$O2013Ratio <- scoring$O2013_rl28_users/scoring$OPP_rl28_users
scoring$O2010Ratio <- scoring$O2010_rl28_users/scoring$OPP_rl28_users

scoring[is.na(scoring)] <- 0 
scoring <- rapply(scoring, f=function(x) ifelse(is.nan(x),0,x), how="replace" )
scoring <- rapply(scoring, f=function(x) ifelse(is.infinite(x),0,x), how="replace" )

write.csv(scoring, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Usage\\Scoring_Perpetual.csv", row.names =FALSE)
