treatment <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\FinalDROutput\\Experimentation\\M365UpsellModelFinalOutputTreatment.csv")
sales_play <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\MSXiCohorts.xlsx")
sales_play <- sales_play %>% select(TPID, `Box #`)
colnames(sales_play)[2] <- "Tranche"
treatment <- inner_join(treatment, sales_play)

tranche_mapping <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\SalesPlays.xlsx")
treatment <- inner_join(tranche_mapping, treatment, by = c("Product", "Tranche"))

write.csv(treatment, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\M365CaUpdatedOutputFY20.csv", row.names = FALSE)


a <- c('Tenure', 'Annuity', 'AIPPaidAvailableUnits', 'MIPPaidAvailableUnits',
      'AnnuityPercent', 'IsFastTrackTenant', 'MIGPaidAvailableUnits',
      'EMSM365E3FUSL', 'TeamsPaidAvailableUnits', 'ProPlusPaidAvailableUnits',
      'O365PlanE3Mean', 'o365E3Perc', 'O365PlanE3', 'OD4BPaidAvailableUnits',
      'DesktopPerpetualMAUMean', 'DesktopProPlusMAU',
      'AudioConferencePaidAvailableUnits', 'HasEMSSku',
      'PhoneSystemPaidAvailableUnits', 'EMSPaidAvailableUnits',
      'DesktopProPlusMAUMean', 'O365M365E3FUSLMean', 'AADPUsageCount',
      'DesktopPerpetualMAU', 'SPOMean',
      'DesktopSubscriptionProPlusUsagePaidPerc', 'EXOMean', 'SPO',
      'PerpetualProPlusUsagePaidPerc', 'O365PlanE3CloudAddOn', 'IntuneMAD',
      'HasWebex', 'm365e3Perc', 'PowerBIProEnabledUsers',
      'PowerBIProPaidAvailableUnits', 'SecondPartyTrend',
      'M365PaidAvailableUnits', 'IntuneUsagePaidPerc', 'EMSM365E3FUSLTrend',
      'OATP', 'IntuneUsageEnabledPerc', 'MIP', 'OATPPaidAvailableUnits',
      'TrialSubscriptionsCount', 'AdvancedThreatProtectionPlan1',
      'EMSE3SuiteCAO', 'AATPPaidAvailableUnits', 'HasM365Sku', 'emse3perc',
      'PowerBIOfficeSuites', 'CustomerLockboxPaidAvailableUnits',
      'SideloadedAppUnknownMean', 'O365E5M365E5FUSLMean', 'o365e5Perc',
      'O365PlanE5Mean', 'AzureActiveDirectory', 'PowerBIStandaloneProTrend',
      'HasM365', 'm365e5perc', 'EMSE5SuiteFUSLMean')

a <- data.frame(a)
colnames(a) <- "feature60"

b <- c('Annuity', 'Tenure', 'AnnuityPercent', 'EMSM365E3FUSL', 'o365E3Perc',
       'ProPlusPaidAvailableUnits', 'IsFastTrackTenant', 'HasEMSSku',
       'IntuneEnabledUsers', 'AADPUsageCount', 'PerpetualProPlusUsagePaidPerc',
       'emse3perc', 'ProPlusUsagePaidPerc', 'O365PlanE3CloudAddOn',
       'TrialAvailableUnits', 'IntuneUsageEnabledPerc', 'EMSM365E3FUSLTrend',
       'TrialSubscriptionsCount', 'OATP', 'M365PaidAvailableUnits', 'HasWebex',
       'PowerBIOfficeSuites', 'SecondPartyTrend', 'EMSE3SuiteCAO',
       'IntuneUsagePaidPerc', 'AdvancedThreatProtectionPlan1',
       'PowerBIOfficeSuitesMean', 'PowerBIStandaloneProTrend',
       'EMSE5SuiteFUSLMean', 'HasM365SKUE5', 'EMSE5', 'EMSE5Mean',
       'm365f1perc', 'EMSE5TotalE5', 'EMSE5SuiteFUSL', 'HasOfficeSKUE5',
       'SideloadedAppUnknown', 'LOB', 'emse5perc', 'EMSM365F1Trend',
       'O365PlanE5CloudAddOn', 'ThirdPartyActiveUniqueUserCountMean',
       'PowerBIPremium', 'PowerBIPremiumPaidAvailableUnits')

b <- data.frame(b)
colnames(b) <- "feature40"

c <- setdiff(a$feature60, b$feature40)
