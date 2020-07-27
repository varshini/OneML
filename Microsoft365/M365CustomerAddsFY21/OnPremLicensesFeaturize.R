library(readxl)
library(dplyr)
library(tidyverse)
library(data.table)
require(TTR)
require(quantmod)
require(caTools)
options(stringsAsFactors = FALSE)

smctpids <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\SMCTPIDs.csv")
colnames(smctpids)[1] <- "FinalTPID"
smctpids <- filter(smctpids, FY21.Subsegment %in% c("SM&C Government - Corporate", "Enterprise Growth", "SM&C Commercial - Corporate") ) #27980 Tpids
smctpids$FinalTPID <- as.character(smctpids$FinalTPID)

licenses <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\OnPremdataFinal.csv")
#licenses <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\OnPremSeatsMonthly.xlsx", sheet = "Sheet2")
colnames(licenses)[1] <- "FinalTPID"
licenses$FinalTPID <- as.character(licenses$FinalTPID)
licenses <- left_join(smctpids, licenses, by = "FinalTPID")
licenses[is.na(licenses)] <- 0

preprocessDate <- function(data)
{
  data$ym <- case_when(data$ym %like%  "Sep2018" ~ "201809", 
                        data$ym %like% "Dec2018" ~ "201812",
                        data$ym %like% "Mar2019" ~ "201903",
                        data$ym %like% "Jun2019" ~ "201906",
                        data$ym %like% "Sep2019" ~ "201909",
                        data$ym %like% "Dec2019" ~ "201912",
                        data$ym %like% "Mar2020" ~ "202003",
                        data$ym %like% "Jun2020" ~ "202006")
  return(data)
}

# Data processing #
annuity <- licenses %>% select(FinalTPID, AnnuitySep2018, AnnuityDec2018, AnnuityMar2019, 
                               AnnuityJun2019, AnnuitySep2019, AnnuityDec2019, AnnuityMar2020, AnnuityJun2020)
annuity <- gather(annuity, ym, Annuity, AnnuitySep2018:AnnuityJun2020)
annuity <- preprocessDate(annuity) %>% select(FinalTPID, ym, Annuity)

nonannuity <- licenses %>% select(FinalTPID, NonAnnuitySep2018, NonAnnuityDec2018, NonAnnuityMar2019, 
                                  NonAnnuityJun2019, NonAnnuitySep2019, NonAnnuityDec2019, NonAnnuityMar2020, NonAnnuityJun2020)
nonannuity <- gather(nonannuity, ym, NonAnnuity, NonAnnuitySep2018:NonAnnuityJun2020)
nonannuity <- preprocessDate(nonannuity) %>% select(FinalTPID, ym, NonAnnuity)

darkannuity <- licenses %>% select(FinalTPID, DarkAnnuitySep2018, DarkAnnuityDec2018, DarkAnnuityMar2019, 
                                  DarkAnnuityJun2019, DarkAnnuitySep2019, DarkAnnuityDec2019, DarkAnnuityMar2020, DarkAnnuityJun2020)
darkannuity <- gather(darkannuity, ym, DarkAnnuity, DarkAnnuitySep2018:DarkAnnuityJun2020)
darkannuity <- preprocessDate(darkannuity) %>% select(FinalTPID, ym, DarkAnnuity)

onprem_seats <- full_join(annuity, nonannuity, by = c("FinalTPID", "ym")) %>% full_join(., darkannuity, by = c("FinalTPID", "ym"))
onprem_seats[is.na(onprem_seats)] <- 0

# Calculating Trends, Mean, Actual, Ratios #
Trending <- function(x){
  n <- length(x)
  x[ x == 0 ] <- 1
  if(n<6 || sum(x>0)<6 || any(is.na(x))) return(rep(0, n))
  cts <- caTools::runmean(x, 3, align = 'right')
  ROC(cts, 1, type = "discrete")
}

featurizeOnPremSeats <- function(cohort_data, cohort_ym)
{
  cohort_data <- cohort_data %>% filter(ym <= cohort_ym)
  features <- cohort_data %>% 
    group_by(FinalTPID) %>% 
    arrange(ym) %>% 
    mutate(AnnuityTrend = Trending(Annuity),
           AnnuityMean = mean(Annuity), 
           NonAnnuityTrend = Trending(NonAnnuity),
           NonAnnuityMean = mean(NonAnnuity), 
           DarkAnnuityTrend = Trending(DarkAnnuity),
           DarkAnnuityMean = mean(DarkAnnuity))
  
  features <- features %>% filter(ym == cohort_ym)
  features$AnnuityPercent <- features$Annuity/(features$Annuity + features$NonAnnuity + features$DarkAnnuity)
  features$NonAnnuityPercent <- features$NonAnnuity/(features$Annuity + features$NonAnnuity + features$DarkAnnuity)
  features$DarkAnnuityPercent <- features$DarkAnnuity/(features$Annuity + features$NonAnnuity + features$DarkAnnuity)
  return (features)
}

####### TRAINING PERIOD #########

seats_training <- featurizeOnPremSeats(onprem_seats, "201912") #Trends, Mean, Actual Snapshots, Ratios
seats_training[is.na(seats_training)] <- 0 
seats_training <- rapply(seats_training, f=function(x) ifelse(is.nan(x),0,x), how="replace" )
seats_training <- rapply(seats_training, f=function(x) ifelse(is.infinite(x),0,x), how="replace" )
write.csv(seats_training, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Seats\\Training_OnPremSeats.csv", row.names = FALSE)

####### SCORING PERIOD #########

seats_scoring <- featurizeOnPremSeats(onprem_seats, "202006") #Trends, Mean, Actual Snapshots, Ratios
seats_scoring[is.na(seats_scoring)] <- 0 
seats_scoring <- rapply(seats_scoring, f=function(x) ifelse(is.nan(x),0,x), how="replace" )
seats_scoring <- rapply(seats_scoring, f=function(x) ifelse(is.infinite(x),0,x), how="replace" )
write.csv(seats_scoring, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Seats\\Scoring_OnPremSeats.csv", row.names = FALSE)

