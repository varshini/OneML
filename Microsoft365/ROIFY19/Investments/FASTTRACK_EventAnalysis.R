library(tidyr)

features <- fread("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\CoreWorkloadFeatures.csv")
features_long <- gather(features, Eventname, Months, AADPTaskServiceTaskAADPt10_x:Task3268_Teams_AssessandEnableMicrosoftTeams_FTCTasksTeamst10_x, factor_key=TRUE)
features_long <- features_long %>% select(TenantId, Eventname, Months, CoreWklMauPrev, CoreWklPAPrev, CoreWklMau)
features_long$Eventname <- str_replace(features_long$Eventname, "t10_x", "")

features_long$EventCategory <- case_when(grepl("ServiceTask", features_long$Eventname) == TRUE ~ "Task", 
                                         grepl("BOTDrivenWizards", features_long$Eventname) == TRUE ~ "Wizards",
                                         grepl("FTMigrationData", features_long$Eventname) == TRUE ~ "Migration",
                                         grepl("AdminCenterWizards", features_long$Eventname) == TRUE ~ "Wizards",

                                         grepl("FTPlaybookMilestones", features_long$Eventname) == TRUE ~ "Playbook",
                                         grepl("FTCTasks", features_long$Eventname) == TRUE ~ "Task",
                                         grepl("BOT", features_long$Eventname) == TRUE ~ "BOT",
                                         grepl("SSAT", features_long$Eventname) == TRUE ~ "SSAT",
                                         grepl("AdoptionWorkshop", features_long$Eventname) == TRUE ~ "Workshop",
                                         grepl("FTPlaybookEvents", features_long$Eventname) == TRUE ~ "Playbook",
                                         grepl("FTOPWizard", features_long$Eventname) == TRUE ~ "Wizards",
                                         grepl("E_Mail", features_long$Eventname) == TRUE ~ "Email")

features_long1 <- features_long %>% filter(Months > 0)
features_long1$PrevUsage <- features_long1$CoreWklMauPrev/features_long1$CoreWklPAPrev
features_long1$CurrUsage <- features_long1$CoreWklMau/features_long1$CoreWklPAPrev
features_long1$MauGrowthPerc <- (features_long1$CoreWklMau - features_long1$CoreWklMauPrev)/features_long1$CoreWklMauPrev
features_long1$MauGrowthPerc[is.infinite(features_long1$MauGrowthPerc)] <- 0
  
## Stats ##
#stats1 <- features_long1 %>% group_by(EventCategory) %>% summarise(count = n())
stats2 <- features_long1 %>% group_by(EventCategory) %>% summarise(AvgMauGrowth = mean(MauGrowthPerc),
                                                                   MedMauGrowth = median(MauGrowthPerc))

# long to wide 
data_wide <- spread(features_long1, EventCategory, Months)
data_wide[is.na(data_wide)] <- 0
data_wide1 <- data_wide %>% group_by(TenantId) %>% summarise(Task = max(Task), Wizards = max(Wizards), Playbook = max(Playbook), BOT = max(BOT), 
                                                            Workshop = max(Workshop), Migration = max(Migration), SSAT = max(SSAT), Email = max(Email), 
                                                            CoreWklPAPrev = max(CoreWklPAPrev), MauGrowthPerc = max(MauGrowthPerc), 
                                                            CoreWklMauPrev = max(CoreWklMauPrev), CoreWklMau = max(CoreWklMau))
noinv_outputs <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\NoInvOutputs.csv")
noinv_outputs <- select(noinv_outputs, TenantId, MAUpred_NoInv)
data_wide_2<- inner_join(data_wide1, noinv_outputs, by = "TenantId")

nrow(filter(data_wide1, Playbook > 0 & Task > 0))
