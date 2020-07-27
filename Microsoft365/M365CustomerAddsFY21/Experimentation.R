library(readxl)
library(splitstackshape)


model_output <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Modeling\\ModelOutputWithPerpetual.csv") 
scoring_data <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\ScoringFeaturesWithPerpetual.csv")

tranches <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\MSXiCohorts.xlsx")
tranches <- tranches %>% select(TPID, `Box #`)
colnames(tranches) <- c("FinalTPID", "Tranche")

model_output1 <- inner_join(model_output, training_data)
model_output1 <- inner_join(tranches, model_output1, by = "FinalTPID")
model_output1$TotalM365 <- model_output1$M365F1 + model_output1$M365E3 + model_output1$M365E5

#Propensity
model_output1$Propensity <- case_when(model_output1$Probability1 >= 0.41 ~ "High",
                                      model_output1$Probability1 >= 0.27 & model_output1$Probability1 < 0.41 ~ "Medium")

model_output1 <- model_output1 %>% filter(Propensity == "High" | Propensity == "Medium")

# Adds/Expansion - M365 or tranches??
model_output1$Scenario <- case_when()

#Tranche??

# sum of M365 seats 

# Area??

# Stratified sample - 80/20 I can detect 2.5% with 95% significance - TODO
sample <- stratified(data, c("Propensity", "Scenario", "M365Bucket", "Tranche", "Area"), 0.80, bothSets = TRUE, replace = FALSE)
nca_treatment_sample <- sample[[1]]
nca_control_sample <- sample[[2]]

write.csv(nca_treatment_sample, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Modeling\\TreatmentAccounts.csv", row.names = F)
write.csv(nca_control_sample, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Modeling\\ControlAccounts.csv", row.names = F)
