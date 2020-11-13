smc_dr_output <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\DROutputTreatment.csv")

smc_dr_output <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\MergedOuputPreExp.csv", sep = ';')
smc_dr_output <- smc_dr_output %>% select(FinalTPID, Recommendation, Propensity)

gtm_rev <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\PotentialRevenue\\GTMCalc.xlsx")
gtm_rev <- gtm_rev %>% select(TPID, SEGMENT, `Rev Potential`, `Incremental Rev`)
gtm_rev_smc <- gtm_rev %>% filter(SEGMENT != "SM&C Corporate") #25844
colnames(gtm_rev_smc)[1] <- "FinalTPID"

final <- inner_join(gtm_rev_smc, smc_dr_output, by = "FinalTPID") #14194

sum(gtm_rev_smc$`Rev Potential`) #9,742,092,128
sum(gtm_rev_smc$`Incremental Rev`) #6,436,037,083

sum(final$`Incremental Rev`) #4564710101

final$PropensityRevenue <- final$`Incremental Rev` * (final$Propensity/100)
sum(final$PropensityRevenue) #2348588907

######################################

ent_output <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\DROutputTreatment.csv")
ent_output <- ent_output %>% select(FinalTPID, Recommendation, Propensity)

gtm_rev <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\PotentialRevenue\\GTMCalc.xlsx")
gtm_rev <- gtm_rev %>% select(TPID, SEGMENT, `Rev Potential`, `Incremental Rev`)
gtm_rev_smc <- gtm_rev %>% filter(SEGMENT == "SM&C Corporate") #25844
colnames(gtm_rev_smc)[1] <- "FinalTPID"

final <- inner_join(gtm_rev_smc, smc_dr_output, by = "FinalTPID") #11331

sum(gtm_rev_smc$`Rev Potential`) #9,742,092,128
sum(gtm_rev_smc$`Incremental Rev`) #6,436,037,083

sum(final$`Incremental Rev`) #3,635,610,878

final$PropensityRevenue <- final$`Incremental Rev` * (final$Propensity/100)
sum(final$PropensityRevenue) #1,830,341,621
