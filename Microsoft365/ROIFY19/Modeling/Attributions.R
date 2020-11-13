# # Rolling upto TPID level 
# data_tpid <- data %>% group_by(Tpid) %>% summarise(
#   AllUpprev = sum(AllUpprev), 
#   AllUp = sum(AllUp), 
#   MAUpred = sum(MAUpred), 
#   MAUpred_Noinv = sum(MAUpred_Noinv), 
#   MAUpred_NoFT= sum(MAUpred_NoFT), 
#   MAUpred_NoCSM = sum(MAUpred_NoCSM), 
#   MAUpred_NoECIF = sum(MAUpred_NoECIF), 
#   MAUpred_NoPARTNER = sum(MAUpred_NoPARTNER)
# )
# 
# file_name <- paste0("C:\\Users\\varamase\\Documents\\DataStreams\\ROI\\V2\\UsageToRevenueTPIDLevel_Dec18.csv")
# write.csv(data_tpid, file_name, row.names = FALSE)

data <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Modeling\\Outputs\\SampleOutput.csv")

## stats for the per investment attribution ##
sum(data$SumMauPrev)
sum(data$SumMau)
sum(data$MAUpred)
sum(data$MAUpred) - sum(data$SumMauPrev)
sum(data$MAUpred) - sum(data$MAUpred_Noinv)

pred_growth <- sum(data$MAUpred) - sum(data$SumMauPrev)
pred_growth
all_attr <- sum(data$MAUpred) - sum(data$MAUpred_NoInv)
all_attr
ft_attr <- sum(data$MAUpred) - sum(data$MAUpred_NoFT)
ft_attr
csm_attr <- sum(data$MAUpred) - sum(data$MAUpred_NoCSM)
csm_attr
ecif_attr <- sum(data$MAUpred) - sum(data$MAUpred_NoECIF)
ecif_attr
partner_attr <- sum(data$MAUpred) - sum(data$MAUpred_NoPARTNER)
partner_attr
ats_attr <- sum(data$MAUpred) - sum(data$MAUpred_NoATS)
ats_attr
services_attr <- sum(data$MAUpred) - sum(data$MAUpred_NoSERVICES)
services_attr

attr <- ft_attr+csm_attr+ecif_attr+partner_attr+ats_attr+services_attr
attr

(ft_attr/attr) * all_attr
ft_attr/attr
(csm_attr/attr) * all_attr
csm_attr/attr
(ecif_attr/attr) * all_attr
ecif_attr/attr
(partner_attr/attr) * all_attr 
partner_attr/attr
(ats_attr/attr) * all_attr 
ats_attr/attr
(services_attr/attr) * all_attr 
services_attr/attr
