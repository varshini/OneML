library(readxl)
library(splitstackshape)


model_output <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Modeling\\DROutput\\FinalDROutput.csv") 

scoring_data <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\ScoringFeaturesFinalv2.csv")

model_output1 <- inner_join(model_output, scoring_data)
model_output1$TotalM365 <- model_output1$M365F1 + model_output1$M365E3 + model_output1$M365E5



#Adds/Expansion
model_output1$adds_exp_bucket <- case_when(model_output1$Recommendation %in% c("High probability to expand 300+ M365 seats in the next 6 months", "Medium probability to expand 300+ M365 seats in the next 6 months") ~ "Expansion", 
                                           model_output1$Recommendation %in% c("High probability to purchase 300+ M365 seats in the next 6 months", "Medium probability to purchase 300+ M365 seats in the next 6 months") ~ "Purchase")

#Propensity
model_output1$propensity_bucket <- case_when(model_output1$Recommendation %in% c("High probability to expand 300+ M365 seats in the next 6 months", "High probability to purchase 300+ M365 seats in the next 6 months") ~ "HighPropensity", 
                                           model_output1$Recommendation %in% c("Medium probability to expand 300+ M365 seats in the next 6 months", "Medium probability to purchase 300+ M365 seats in the next 6 months") ~ "MediumPropensity")


#Tranche
model_output1$tranche_bucket <- model_output1$Tranche

# Area
area <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Modeling\\DROutput\\Area.xlsx")
colnames(area)[1] <- "FinalTPID"
area$FinalTPID <- as.integer(area$FinalTPID)
model_output1 <- inner_join(area, model_output1)

#sum of M365 seats 
model_output1$M365_Bucket <- case_when(model_output1$TotalM365 <= 0 ~ "0", 
                                       model_output1$TotalM365 > 0 & model_output1$TotalM365 < 150 ~ "1-150", 
                                       model_output1$TotalM365 >= 150 & model_output1$TotalM365 < 300 ~ "150-300",
                                       model_output1$TotalM365 >= 300 & model_output1$TotalM365 < 700 ~ "300-700", 
                                       model_output1$TotalM365 >= 700 & model_output1$TotalM365 < 2000 ~ "700-2000", 
                                       model_output1$TotalM365 >= 2000 ~ ">2000" )

model_output1 <- model_output1 %>% select(FinalTPID, adds_exp_bucket, propensity_bucket, tranche_bucket, AreaName, M365_Bucket)


# Stratified sample - 80/20 I can detect 2% with 95% significance
sample <- stratified(model_output1, c("adds_exp_bucket", "propensity_bucket", "tranche_bucket", "M365_Bucket", "AreaName"), 0.80, bothSets = TRUE, replace = FALSE)
nca_treatment_sample <- sample[[1]]
nca_control_sample <- sample[[2]]


a <- nca_treatment_sample %>% group_by(AreaName) %>% summarise(count = n())
b <- model_output1 %>% group_by(AreaName) %>% summarise(count = n())

nca_treatment_sample <- nca_treatment_sample %>% select(FinalTPID) 
nca_treatment_sample <- inner_join(nca_treatment_sample, model_output, by = "FinalTPID") %>% select(FinalTPID, Recommendation, WhyRecommended, Propensity, Product, SalesPlay)
nca_treatment_sample$ModelName <- "M365UpsellModel"
nca_treatment_sample$CreatedDate <- "7/28/2020"

write.csv(nca_treatment_sample, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Modeling\\DROutput\\TreatmentAccounts.csv", row.names = F)
write.csv(nca_control_sample, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Modeling\\DROutput\\ControlAccounts.csv", row.names = F)